package com.bupt.mydb.backend.dm;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.dm.logger.Logger;
import com.bupt.mydb.backend.dm.page.Page;
import com.bupt.mydb.backend.dm.page.PageX;
import com.bupt.mydb.backend.dm.pageCache.PageCache;
import com.bupt.mydb.backend.tm.TransactionManager;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.*;

/**
 * @author gao98
 * date 2025/9/19 22:15
 * description:
 * Recover类是数据库系统的崩溃恢复核心模块，负责在数据库启动时恢复数据一致性。
 * 恢复三阶段:
 * 确定最大页号：扫描日志找到最大页面ID
 * 重做阶段：重新执行已提交事务的操作
 * 回滚阶段：撤销未提交事务的操作
 * <p>
 * 恢复策略
 * 数据库操作限制
 * 为了避免冲突和脏读问题，有如下限制数据库操作的规则：
 * <p>
 * 正在进行的事务，不会读取其他任何未提交的事务产生的数据
 * 正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据
 * 为此，数据恢复依赖于如上规则：
 * <p>
 * 通过redo log重做所有崩溃时已完成的事务（即状态为commited或aborted的事务）
 * 通过undo log撤销所有崩溃时未完成的事务（即状态为active的事务）
 * <p>
 * redo log
 * 恢复流程
 * 正序扫描事务T的所有日志
 * 如果日志是插入操作(Ti, I, A, x)，那么将x重新插入到A位置
 * 如果日志是更新操作(Ti, U, A, oldx, newx)，那么将A位置的值设置为newx
 * undo log
 * 恢复流程
 * 倒序扫描事务T的所有日志
 * 如果日志是插入操作(Ti, I, A, x)，那么将A位置的数据删除
 * 如果日志是更新操作(Ti, U, A, oldx, newx)，那么将A位置的值设置为oldx
 * <p>
 * 插入日志&更新日志
 * 上面的redo log和undo log是从恢复角度来分类的，而每条日志又可以是插入日志或者更新日志
 * <p>
 * 插入日志格式
 * [LogType] [XID] [Pgno] [Offset] [Raw]
 * <p>
 * 更新日志格式
 * [LogType] [XID] [UID] [OldRaw] [NewRaw]
 */
public class Recover {

    /**
     * 日志类型标识
     */
    // 插入日志,标识一条"数据插入"类型的日志记录
    private static final byte LOG_TYPE_INSERT = 0;
    // 更新日志,标识一条"数据更新"类型的日志记录
    private static final byte LOG_TYPE_UPDATE = 1;

    /**
     * 恢复操作常量
     */
    //REDO- 重做操作,崩溃恢复时重新执行已提交事务的操作,保证已提交事务的持久性(Durability)
    private static final int REDO = 0;
    //UNDO- 回滚操作,回滚未提交事务的操作,保证事务的原子性(Atomicity)
    private static final int UNDO = 1;

    // 每条日志项的数据格式：[LogType] [XID] [UID] [OldRaw] [NewRaw]
    //| 日志类型(1)| XID(8) | UID(8) | 旧数据(N)      | 新数据(N)       |
    // 日志类型偏移(1字节)
    // LogType 日志类型的偏移位置
    private static final int OF_TYPE = 0;
    // 事务ID偏移(+8字节)
    // XID 日志中xid的偏移位置
    private static final int OF_XID = OF_TYPE + 1;

    // UID偏移(+8字节)
    // UID    更新日志中UID的偏移位置
    private static final int OF_UPDATE_UID = OF_XID + 8;
    // 数据偏移
    // UpdateLogOffsetOldRaw 更新日志中旧数据项的偏移位置l
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;


    // [LogType] [XID] [Pgno] [Offset] [Raw]
    // 页号偏移量(事务ID后8字节)
    // InsertLogOffsetPageNumber 插入日志中关联的页号的偏移位置
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    // 偏移量位置(页号后4字节)
    // InsertLogOffsetOffset 插入日志中关联的页内偏移的偏移位置
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    // 数据起始位置(偏移量后2字节)
    // InsertLogOffsetRaw 插入日志中数据项的偏移位置
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    /**
     * 日志类型定义
     */
    static class InsertLogInfo {
        // 事务ID
        long xid;
        // 页面号
        int pgno;
        // 页面内偏移
        short offset;
        // 插入数据
        byte[] raw;
    }

    static class UpdateLogInfo {
        // 事务ID
        long xid;
        // 页面号
        int pgno;
        // 页面内偏移
        short offset;
        // 旧数据
        byte[] oldRaw;
        // 新数据
        byte[] newRaw;
    }

    /**
     * 主恢复方法
     * 确定截断位置：通过扫描日志找到最大页面号
     * 文件截断：确保数据文件与日志匹配
     * 重做：恢复已提交事务的修改
     * 回滚：撤销未提交事务的修改
     *
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    public static void recover(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        System.out.println("Recovering......");

        logger.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) {
                break;
            }
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pgno = insertLogInfo.pgno;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pgno = updateLogInfo.pgno;
            }
            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        pageCache.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(transactionManager, logger, pageCache);
        System.out.println("Redo Transactions Over.");

        undoTransactions(transactionManager, logger, pageCache);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * isInsertLog()是一个简单的日志类型判断方法，用于快速区分插入日志和更新日志。
     *
     * @param log
     * @return
     */
    // IsInsertLog 判断是否是插入日志
    private static boolean isInsertLog(byte[] log) {
        //如果首字节为0则为插入日志,否则为false
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * redoTransactions()是数据库恢复过程中的重做(Redo)阶段实现，负责重新执行所有已提交事务的操作。
     *
     * @param tm 事务管理器（TransactionManager）
     * @param lg 日志记录器（Logger）
     * @param pc 页面缓存（PageCache）
     */
    // redoTransactions 遍历事务，根据事务的状态决定是否要进行redo操作(包括了插入的redo和更新的redo)
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        //将日志读取指针重置到日志文件开头，确保从头开始扫描所有日志记录
        // 重置日志文件的读取位置到文件的4的位置，因为前4个字节是日志文件的校验和
        lg.rewind();
        // 循环读取日志文件中的所有日志记录
        while (true) {
            //循环读取每条日志记录，lg.next()返回null表示日志结束
            // 读取下一条日志记录
            byte[] log = lg.next();
            // 如果读取到的日志记录为空，表示已经读取到日志文件的末尾，跳出循环
            if (log == null) {
                break;
            }
            // 判断日志记录的类型
            if (isInsertLog(log)) {
                // 处理插入日志
                // 如果是插入日志，解析日志记录，获取插入日志信息
                InsertLogInfo li = parseInsertLog(log);
                // 获取事务ID
                long xid = li.xid;
                //只重做已提交事务（非活跃事务），活跃事务将在Undo阶段处理
                // 如果当前事务已经提交，进行重做操作
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 处理更新日志
                // 如果是更新日志，解析日志记录，获取更新日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                // 获取事务ID
                long xid = xi.xid;
                // 如果当前事务已经提交，进行重做操作
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * undoTransactions()是数据库恢复过程中的回滚(Undo)阶段实现，负责撤销所有未提交事务的操作。
     *
     * @param tm 事务管理器（TransactionManager）
     * @param lg 日志记录器（Logger）
     * @param pc 页面缓存（PageCache）
     */
    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        // 创建事务日志缓存（事务ID → 日志列表）
        // 创建一个用于存储日志的映射，键为事务ID，值为日志列表
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        //重置日志读取位置
        // 将日志文件的读取位置重置到开始
        lg.rewind();
        // 循环读取日志文件中的所有日志记录
        while (true) {
            //遍历所有日志记录，按事务ID分类缓存活跃事务的日志
            // 读取下一条日志记录
            byte[] log = lg.next();
            // 如果读取到的日志记录为空，表示已经读取到日志文件的末尾，跳出循环
            if (log == null) {
                break;
            }
            // 判断日志记录的类型
            if (isInsertLog(log)) {
                // 如果是插入日志，解析日志记录，获取插入日志信息
                InsertLogInfo li = parseInsertLog(log);
                // 获取事务ID
                long xid = li.xid;
                // 只缓存未提交事务（活跃事务）的日志
                // 如果当前事务仍然活跃，将日志记录添加到对应的日志列表中
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    //使用HashMap按事务ID分组
                    logCache.get(xid).add(log);
                }
            } else {
                // 如果是更新日志，解析日志记录，获取更新日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                // 获取事务ID
                long xid = xi.xid;
                //只缓存未提交事务（活跃事务）的日志
                // 如果当前事务仍然活跃，将日志记录添加到对应的日志列表中
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    //使用HashMap按事务ID分组
                    logCache.get(xid).add(log);
                }
            }
        }
        // 对所有active log进行倒序undo
        // 对所有上面记录的事务进行undo操作
        // 对所有活跃的事务的日志进行倒序撤销
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            //逆序处理：从最新到最旧
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                //执行回滚：doInsertLog/doUpdateLog with UNDOflag
                // 判断日志记录的类型
                if (isInsertLog(log)) {
                    // 如果是插入日志，进行撤销插入操作
                    doInsertLog(pc, log, UNDO);
                } else {
                    // 如果是更新日志，进行撤销更新操作
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 中止当前事务
            tm.abort(entry.getKey());
        }
    }


    /**
     * 生成更新日志
     * 日志示例：
     * [1][0,0,0,0,0,0,0,1][0,0,0,1,0,0,0,16][0xAA,0xBB][0xCC,0xDD]
     * 表示：
     * - 事务1在页面1偏移16处
     * - 将数据从[0xAA,0xBB]修改为[0xCC,0xDD]
     *
     * @param xid
     * @param di
     * @return
     */
    public static byte[] updateLog(long xid, DataItem di) {
        // 日志类型标记(1字节)
        byte[] logType = {LOG_TYPE_UPDATE};
        // 事务ID(8字节)
        // xid 长度为8
        byte[] xidRaw = Parser.long2Byte(xid);
        // UID(8字节) 高32位存储页号，低16位存储偏移量
        // uid 长度为8
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        // 旧数据，同时保存修改前后的完整数据
        // oldRaw 长度为len(di.GetOldRaw())
        byte[] oldRaw = di.getOldRaw();
        // newRaw 长度为len(di.GetRaw())
        // 新数据范围
        SubArray raw = di.getRaw();
        // 新数据
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        //使用Guava的Bytes.concat高效拼接
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 解析更新日志
     *
     * @param log
     * @return UID解码
     * UID结构示例：0x0000000100000010
     * 高32位：0x00000001 → 页号1
     * 低16位：0x0010 → 偏移16
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        // 解析事务ID(字节1-8)
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        // 解析UID(字节9-16)
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        // uid中低16位是偏移量
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        // uid中高32位是页号
        li.pgno = (int) (uid & ((1L << 32) - 1));
        // 计算数据长度(旧数据和新数据各占一半)
        // 这里oldRaw和newRaw的数据长度应该是一样的
        int length = (log.length - OF_UPDATE_RAW) / 2;
        //等分获取新旧数据
        // 解析旧数据
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        // 解析新数据
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    /**
     * doUpdateLog()是数据库恢复系统中执行更新日志操作的核心方法，负责实际应用日志记录到数据页面
     * 执行更新日志
     *
     * @param pc   页面缓存管理器（PageCache）
     * @param log  二进制日志记录
     * @param flag 恢复标志（REDO/UNDO）
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        // 存储页面编号
        int pgno;
        // 存储页内偏移量
        short offset;
        // 存储数据项的原始数据
        byte[] raw;
        // 根据标志位判断是redo操作还是undo操作
        if (flag == REDO) {
            // 如果是重做操作，解析日志记录，获取更新日志信息，主要获取新数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            // 重做使用新数据
            raw = xi.newRaw;
        } else {
            // 如果是撤销操作，解析日志记录，获取更新日志信息，主要获取旧数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            // 回滚使用旧数据
            raw = xi.oldRaw;
        }
        // 通过PageCache获取页面
        // 错误处理：页面获取失败直接终止系统，使用try块隔离可能异常
        // 用于存储获取到的页面，尝试从页面缓存中获取指定页码的页面
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            //调用PageX工具类执行实际更新，pg：目标页面，raw：要写入的数据，offset：页面内偏移量
            // 在指定的页面和偏移量处插入解析出的数据, 数据页缓存讲解了该方法
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            //资源释放
            // 无论是否发生异常，都要释放页面
            pg.release();
        }
    }


    /**
     * 生成插入日志
     * | 类型(1)| XID(8) | 页号(4)| 偏移(2) | 数据(N) |
     * 日志示例：
     * [0][0,0,0,0,0,0,0,1][0,0,0,1][0,16][0xAA,0xBB]
     * 表示：
     * - 事务1在页面1偏移16处
     * - 插入了数据[0xAA,0xBB]
     * InsertLog 生成插入日志
     *
     * @param xid
     * @param pg
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        // 日志类型标记(1字节)
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        // xid 长度为8
        // 事务ID(8字节)，事务关联：包含事务ID用于恢复
        byte[] xidRaw = Parser.long2Byte(xid);
        // 页号(4字节)
        // pageNumber 长度为4
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        // 空闲位置偏移(2字节)，FSO记录：使用PageX.getFSO()获取页面空闲空间的偏移量
        // offset 长度为2，在一个页面中要插入的位置，肯定得是空闲的，不能已经有数据了。
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        //数据拼接：高效拼接5个部分字节数组
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /**
     * 解析插入日志
     * | 类型(1)| XID(8) | 页号(4)| 偏移(2) | 数据(N) |
     *
     * @param log
     * @return 固定位置解析：按照预定义的偏移量提取数据
     * 类型转换：将字节数据转为Java类型
     * 数据分离：准确分离头部信息和实际数据
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        // 解析事务ID(字节1-8)
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        // 解析页号(字节9-12)
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        // 解析偏移量(字节13-14)
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        // 剩余部分是原始数据(字节15-end)
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * 执行插入日志
     * SetDataItemRawInvalid涉及到数据管理器对文件的管理，
     * 这里先不展开讨论，只需要知道这里是上了个标记，将这个数据项标志为无效，而无效对应的就是删除了，
     * 也就完成了undo日志的恢复
     * PageXRecoverInsert则是将日志的数据写入到页面中去，其中的offset属性就在此时发挥作用了，
     * 可以表明是插入到页面中的什么位置
     *
     * @param pc
     * @param log
     * @param flag
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        // 解析日志，将字节数组转为InsertLogInfo对象
        InsertLogInfo li = parseInsertLog(log);
        // 获取页面
        Page pg = null;
        try {
            //根据日志对象中的页号，找到页面，如果缓存中有，直接拿缓存的，如果缓存没有，就去硬盘里加载，并放入缓存
            pg = pc.getPage(li.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 如果类型是Undo，那么需要将数据项标记为无效
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 执行实际插入
            // 将数据项插入到页面中，这里同时适用于undo和redo类型，因为上面的undo已经将数据项标记为无效了，所以这里会直接插入
            // 将日志的数据写入到页面中去，其中的offset属性就在此时发挥作用了，可以表明是插入到页面中的什么位置
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            // 确保释放页面
            pg.release();
        }
    }
}
