package com.bupt.mydb.backend.vm;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * Entry类表示数据库中的一条记录，是VM(Version Manager)向上层抽象出的数据结构。
 * <p>
 * Entry的结构如下：
 * [XMIN][XMAX][data]
 * <p>
 * XMIN(8字节)：创建该记录的事务ID
 * XMAX(8字节)：删除该记录的事务ID（初始为0，表示未被删除）
 * data(N字节)：实际数据内容
 * <p>
 * XMIN：这是创建该条Entry的事务的编号
 * XMAX：这是删除该条Entry的事务编号
 * data：该条Entry持有的数据
 * <p>
 * 通过维护XMIN和XMAX，实现MVCC（多版本并发控制）机制，
 * 支持事务的隔离性和一致性。
 * 一个版本
 */

/**
 * VM创建entry，为上层服务
 * entry结构：
 * [XMIN] [XMAX] [data]
 */
public class Entry {
    //定义了XMIN的偏移量为0，8个字节标识创建该记录(版本)的编号，这是创建该条Entry的事务的编号
    private static final int OF_XMIN = 0;
    //定义了XMAX的偏移量为XMIN偏移量后的8个字节,8个字节标识删除该记录(版本的)编号，这是删除该条Entry的事务编号
    private static final int OF_XMAX = OF_XMIN + 8;
    //定义了DATA的偏移量为XMAX偏移量后的8个字节,该记录(版本)的数据，该条Entry持有的数据
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    // dataItem DataItem对象，用来存储数据的
    private DataItem dataItem;
    // vm VersionManager对象，用来管理版本的
    private VersionManager vm;

    /**
     * 创建一个新的Entry实例
     *
     * @param vm       版本管理器，用于管理Entry的版本控制
     * @param dataItem 底层数据项，包含实际的数据内容和版本控制信息
     * @param uid      Entry的唯一标识符
     * @return Entry实例或null（如果dataItem为null）
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 根据唯一标识符加载已存在的Entry
     *
     * @param vm  版本管理器
     * @param uid Entry的唯一标识符
     * @return 加载的Entry实例
     * @throws Exception 数据读取异常
     */
    // LoadEntry 用来加载一个Entry。它首先从VersionManager中读取数据，然后创建一个新的Entry
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        // 通过版本管理器读取数据项
        // 注意：实际实现中需要将VersionManager强转为具体实现类以访问数据管理器
        DataItem di = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 将原始数据包装成Entry格式
     * 生成日志格式的Entry数据
     *
     * @param xid  事务ID，作为XMIN值
     * @param data 原始数据
     * @return 包装后的Entry数据格式：[XMIN][XMAX][data]
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        // 将事务id转为8字节数组
        byte[] xmin = Parser.long2Byte(xid);
        // 创建一个空的8字节数组，等待版本修改或删除时才修改
        byte[] xmax = new byte[8];
        // 将XMIN和XMAX拼接到一起，然后拼接上data
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 释放Entry资源
     */
    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }

    /**
     * 删除底层数据项
     */
    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回Entry的实际数据内容（不包括XMIN和XMAX部分）
     * 获取记录中持有的数据
     * 获取的函数Data与WrapEntryRaw其实是相反的操作，Data()中只要把前面的XMIN和XMAX去掉即可
     *
     * @return 实际数据内容的副本
     */
    // 以拷贝的形式返回内容，只返回内容
    public byte[] data() {
        dataItem.rLock();
        try {
            // 获取日志数据
            SubArray sa = dataItem.data();
            // 创建一个去除前16字节的数组，因为前16字节表示 xmin and xmax
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            // 拷贝数据到data数组上
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取创建该记录的事务ID（XMIN）
     *
     * @return 创建该记录的事务ID
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取删除该记录的事务ID（XMAX）
     *
     * @return 删除该记录的事务ID，0表示未被删除
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置删除该记录的事务ID（XMAX），标记记录为已删除
     *
     * @param xid 删除该记录的事务ID
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     * 获取Entry的唯一标识符
     *
     * @return Entry的唯一标识符
     */
    public long getUid() {
        return uid;
    }
}