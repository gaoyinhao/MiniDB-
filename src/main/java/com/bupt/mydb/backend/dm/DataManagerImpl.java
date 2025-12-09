package com.bupt.mydb.backend.dm;

import com.bupt.mydb.backend.common.AbstractCache;
import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.dm.dataitem.DataItemImpl;
import com.bupt.mydb.backend.dm.logger.Logger;
import com.bupt.mydb.backend.dm.page.Page;
import com.bupt.mydb.backend.dm.page.PageOne;
import com.bupt.mydb.backend.dm.page.PageX;
import com.bupt.mydb.backend.dm.pageCache.PageCache;
import com.bupt.mydb.backend.dm.pageIndex.PageIndex;
import com.bupt.mydb.backend.dm.pageIndex.PageInfo;
import com.bupt.mydb.backend.tm.TransactionManager;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.common.Error;
import com.bupt.mydb.common.Types;


/**
 * @author gao98
 * date 2025/9/17 22:16
 * description:
 * DataManagerImpl是数据库系统的数据管理核心实现，负责协调数据存储、事务管理和恢复机制。
 * DM层在操作数据时，使用的是DataItem的形式，而DataManager则是用来对上层其他模块提供数据操作的API的；
 * 另外，DataManager还提供了对DataItem的缓存机制
 */
//UID
//uid是用来唯一标识一个DataItem的
//其长度为8个字节，高4字节的32位表示DataItem存储的Page的页号；低4字节的32位中只有低16位有意义，
// 这16位表示DataItem存储的Page中的页内偏移，而高16位无意义
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    // TM 事务管理器
    TransactionManager transactionManager;
    // PC 页面缓存
    PageCache pageCache;
    //记录操作日志
    // Logger 数据库日志
    Logger logger;
    // PIndex 页面索引 ,功能就是将空闲页面分成区，安装每个页面有多少个区进行分组存储起来，这样用户在存储数据时，
    // 可以根据存储数据的大小需要多少个区，直接去PageIndex按照下标去取就行了
    PageIndex pageIndex;
    // PageOne 第一页
    Page pageOne;

    //创建新的DM
    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager transactionManager) {
        super(0);
        //页面缓存，因为DataManager是对数据进行操作，当然会用到页面
        this.pageCache = pageCache;
        //对数据操作之后得记录日志，当然会用到Logger
        this.logger = logger;
        //同样，对数据操作需要记录事务，同样会用到事务
        this.transactionManager = transactionManager;
        //页面索引，同样，新增数据，需要用到页面的空闲空间，那么PageIndex就必不可少了哦。
        this.pageIndex = new PageIndex();
    }

    /**
     * 通过UID从缓存获取数据项
     * 检查数据项有效性（是否被逻辑删除）
     * 返回有效数据或null
     *
     * @param uid 数据项的唯一标识符（由页面号和偏移量组成）
     * @return
     * @throws Exception UID结构：
     *                   [32位页号][16位偏移量]
     */
    //向上层提供的API
    //读取数据Read
    //根据uid从缓存中读取DataItem，并校验有效位
    @Override
    public DataItem read(long uid) throws Exception {
        // 先尝试从缓存获取
        //这里的Get是使用了模板方法设计模式，规定了获取缓存的流程，调用的还是继承的父类的抽象缓存的get方法
        DataItemImpl di = (DataItemImpl) super.get(uid);
        // 检查有效性
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     * 将数据安全地插入数据库。
     *
     * @param xid  事务ID
     * @param data 要插入的字节数据
     * @return
     * @throws Exception 1. 数据准备与校验；2. 页面选择与分配；3. 数据写入与返回
     */
    //在PageIndex中获取一个足以存储数据的页面的页号
    //获取页面后，首先需要写入日志，接着才可以通过PageX插入数据，插入后返回页内偏移
    //最后需要将页面的信息重新更新回PageIndex
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 数据包装：将原始数据包装成DataItem格式（添加头信息等），转换为数据库存储格式
        // 示例："Hello"→ [valid][length][H][e][l][l][o]。1字节有效性标记；2字节数据长度；实际数据内容
        byte[] raw = DataItem.wrapDataItemRaw(data);
        //大小检查：确保数据不超过单页容量。MAX_FREE_SPACE：页面最大可用空间（通常=页大小-元数据）
        //设置页大小8KB，MAX_FREE_SPACE=8190B（扣除元数据）
        // 数据都大于了页面的理论最大空间，报错；这里注意数据大小不能大于一个页面大小，即8K减去前面元信息
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DATA_TOO_LARGE_EXCEPTION;
        }
        // 2. 页面选择（最多尝试5次），查找现有可用页面，第5次失败后抛出异常
        // 从页面的索引信息中获取一个仍有足够空闲的页面
        PageInfo pi = null;
        // 循环多取几次，一次取不到就创建一个新页面，这样操作5次还是取不到就算了
        for (int i = 0; i < 5; i++) {
            // 从页面索引中选择一个可以容纳新数据项的页面
            //Select作用是从当前的PageIndex中选择一个能够容纳当前要插入的数据的页面
            pi = pageIndex.select(raw.length);
            // 如果找到了合适的页面，跳出循环
            if (pi != null) {
                break;
            } else {
                // 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
                int newPgno = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 如果还是没有找到合适的页面，抛出异常
        if (pi == null) {
            throw Error.DATABASE_BUSY_EXCEPTION;
        }
        // 取出索引的页面信息后获取仍有空闲的页面
        Page pg = null;
        // freeSpace表示该页仍然空闲的大小
        int freeSpace = 0;
        try {
            // 获取页面信息对象中的页面
            pg = pageCache.getPage(pi.getPgno());
            //先写日志到磁盘，确保崩溃可恢复。
            //日志格式
            //[INSERT][xid][pgno][offset][rawData]
            // 生成插入日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            // 将日志写入日志文件
            logger.log(log);
            //在页面FSO位置写入数据，更新FSO指针：newFSO = oldFSO + data.length，返回数据起始偏移量
            // 在页面中插入新的数据项，并获取其在页面中的偏移量
            short offset = PageX.insert(pg, raw);
            // 释放页面
            pg.release();
            //生成UID，UID结构:[32位页号][16位偏移量]。页号=1，偏移=100 → 0x0000000100000064
            // 返回新插入的数据项的唯一标识符，即uid
            return Types.addressToUid(pi.getPgno(), offset);
        } finally {
            // 将取出的pg重新插入pIndex
            // 更新页面索引中的空闲空间信息，释放页面缓存引用，无论成功失败都会执行
            // 将页面重新添加到页面索引中
            // Add则是给页面索引添加一个页面的索引信息
            if (pg != null) {
                pageIndex.add(pi.getPgno(), PageX.getFreeSpace(pg));
            } else {
                pageIndex.add(pi.getPgno(), freeSpace);
            }
        }
    }

    //关闭Close
    //关闭DM，需要执行缓存和日志的关闭流程，还需要设置第一页的字节校验
    @Override
    public void close() {
        //关闭AbstractCache<DataItem>管理的所有数据项
        super.close();
        //关闭日志系统
        logger.close();
        //标记第一页状态，标识数据库已正常关闭
        PageOne.setVcClose(pageOne);
        // 释放第一页资源
        pageOne.release();
        //关闭页面缓存
        pageCache.close();
    }

    /**
     * 为xid生成update日志
     * 生成更新日志
     * 记录数据项修改日志（WAL机制）
     * 执行流程：
     * 1,构造更新日志：[UPDATE][xid][pgno][offset][oldData][newData]
     * 2,写入日志文件
     *
     * @param xid
     * @param dataItem
     */
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    /**
     * 释放数据项,递减数据项引用计数，必要时从缓存移除
     * 缓存策略：引用计数归零时：若为脏数据则刷盘,从缓存中移除
     *
     * @param di
     */
    // DataItem 的缓存释放，需要将 DataItem 写回数据源，
    // 由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 缓存加载
     *
     * @param uid
     * @return
     * @throws Exception
     */
    //GetForCache
    //只需要从 uid 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出数据，封装为DataItem并返回
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 解析偏移量
        short offset = (short) (uid & ((1L << 16) - 1));
        // 解析页号
        uid >>>= 32;
        int pgno = (int) (uid & ((1L << 32) - 1));
        Page pg = pageCache.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * 缓存释放
     * 资源链：DataItem → Page → PageCache
     * 释放顺序：1，释放数据项缓存；2，释放关联页面；3，页面缓存自动管理
     * DataItem 的缓存释放，需要将 DataItem 写回数据源，
     * 由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
     *
     * @param dataItem
     */
    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }


    /**
     * 初始化第一页
     * 在创建文件时初始化PageOne
     * 存储数据库元数据，崩溃恢复标记，版本控制信息
     */
    // InitPageOne 在创建文件时初始化第一页
    void initPageOne() {
        // 确保pgno=1
        int pgno = pageCache.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            //将第一页加载到页面缓存中来
            pageOne = pageCache.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 立即持久化
        pageCache.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性

    /**
     * 校验第一页，比较打开VC和关闭VC是否一致
     *
     * @return
     */
    // LoadCheckPageOne 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            // 读取第一页
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex

    /**
     * 构建页面索引
     * 跳过第一页原因：第一页存储元数据而非常规数据
     */
    // FillPageIndex 初始化pageIndex
    void fillPageIndex() {
        // 获取总页数,总页数是在PageCache类中通过加载db文件，然后除以页大小来得到的页总数
        // 获取当前的pageCache中的页面数量
        int pageNumber = pageCache.getPageNumber();
        // 从第2页开始遍历
        // 遍历从第二页开始的每一页
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                // 获取第i页
                pg = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 获取第i页的空闲空间大小
            // 将第i页的页面编号和空闲空间大小添加到 PageIndex 中
            pageIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // 释放页面
            pg.release();
        }
    }
}
