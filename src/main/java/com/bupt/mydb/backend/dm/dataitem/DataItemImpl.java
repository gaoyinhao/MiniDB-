package com.bupt.mydb.backend.dm.dataitem;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.DataManagerImpl;
import com.bupt.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author gao98
 * date 2025/9/17 20:50
 * description:
 * 这是 DataItem接口的具体实现类，用于管理数据库中的数据项（记录）。
 * | 有效位 (1字节) | 数据长度 (2字节) | 实际数据 (N字节) |
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {


    //  数据项的校验位置， 1字节，0为合法，1为非法
    static final int OF_VALID = 0;

    //  数据项的数据大小位置 2字节，标识Data的长度
    static final int OF_SIZE = 1;

    // DataItemOffsetData 数据项的数据位置
    static final int OF_DATA = 3;

    /**
     * 当前数据内容(引用页面数据的一部分)
     */
    //  raw 原始数据
    private SubArray raw;
    /**
     * 旧数据副本（用于事务回滚）
     */
    // oldRaw 旧数据
    private byte[] oldRaw;
    /**
     * 读锁
     */
    private Lock rLock;
    /**
     * 写锁
     */
    private Lock wLock;
    /**
     * 所属数据管理器
     */
    // dataManager 数据管理器
    private DataManagerImpl dataManager;
    /**
     * UID 结构如下:
     * [pageNumber] [空] [offset]
     * <p>
     * pageNumber 4字节，页号
     * 中间空下2字节
     * offset 2字节，偏移量
     * 这里的UID实际上就是由页号+页内偏移组成的，可以通过此唯一标识一个**DataItem**
     */
    // uid 数据项的UID，唯一标识符
    private Long uid;

    /**
     * 所属页面
     */
    // page 页面对象
    private Page page;

    //创建新的DataItem
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dataManager) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
        this.dataManager = dataManager;
        this.uid = uid;
        this.page = page;
    }

    /**
     * 检查数据项是否有效（有效位为0表示有效）。
     * 判断数据项是否合法IsValid
     * dataItem 结构如下：
     * [ValidFlag] [DataSize] [Data]
     * ValidFlag 1字节，0为合法，1为非法
     * DataSize 2字节，标识Data的长度
     *
     * @return
     */
    // IsValid 判断数据项是否合法，注意0是合法标志
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    /**
     * 获取数据项的实际数据部分（跳过头部）。
     * 注意返回的是切片，这意味着返回的数据和dataItem中的数据是同一份的
     *
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 开始修改前的准备：获取写锁，标记页面为脏页，保存旧数据。
     */
    //在修改数据项之前调用，用于锁定数据项并保存原始数据
    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        //保存原始数据的副本到oldRaw中，以便在需要时进行回滚
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 撤销修改：恢复旧数据，释放写锁。
     * 需要撤销修改时调用，用于恢复原始数据并解锁数据项
     */
    @Override
    public void unBefore() {
        //将oldRaw中的数据复制回raw中，实现回滚操作
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 修改完成：记录日志，释放写锁。
     * 在修改完成之后调用，用于记录日志并解锁数据项
     * 在修改数据项之后调用
     */
    @Override
    public void after(long xid) {
        //LogDataItem的作用是生成对应的操作日志，然后持久化到log文件中
        dataManager.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 释放数据项资源
     * 使用完DataItem后，调用Release释放掉DataItem的缓存
     */
    @Override
    public void release() {
        dataManager.releaseDataItem(this);
    }

    /**
     * 提供排他锁和共享锁的获取/释放方法。
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    /**
     * 提供排他锁和共享锁的获取/释放方法。
     */
    @Override
    public void unLock() {
        wLock.unlock();
    }

    /**
     * 提供排他锁和共享锁的获取/释放方法。
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /**
     * 提供排他锁和共享锁的获取/释放方法。
     */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 各种getter方法。
     *
     * @return
     */
    // Page 返回数据项所在的页面
    @Override
    public Page page() {
        return page;
    }

    /**
     * 各种getter方法。
     *
     * @return
     */
    // UID 返回数据项的UID
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 各种getter方法。
     *
     * @return
     */
    // GetOldRaw 返回数据项的旧数据
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 各种getter方法。
     *
     * @return
     */
    // GetRaw 返回数据项的原始数据
    @Override
    public SubArray getRaw() {
        return raw;
    }
}
