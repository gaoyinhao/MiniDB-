package com.bupt.mydb.backend.dm.page;


import com.bupt.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/17 21:50
 * description:
 * 1. **页面结构定义**：
 * - 页面（Page）是存储在内存中的数据单元，其结构包括：
 * - pageNumber：页面的页号，从**1**开始计数。
 * - data：实际包含的字节数据。
 * - dirty：标志着页面是否是脏页面，在缓存驱逐时，脏页面需要被写回磁盘。
 * - lock：用于页面的锁。
 * - PageCache：保存了一个 PageCache 的引用，方便在拿到 Page 的引用时可以快速对页面的缓存进行释放操作。
 */
// 虽然数据是存储在db文件中的，但是在操作的时候以页面（Page）为单位，
// 所以页面的概念实际上是在内存层面的，在存储层面只是一个db文件
public class PageImpl implements Page {


    /**
     * 当前页面的页号，从1开始计数
     */
    private int pageNumber;
    /**
     * 实际包含的字节数据
     */
    private byte[] data;
    /**
     * 标志该页面是否是脏页面，在驱逐缓存时，需要把脏页面的数据持久化到db文件中去
     */
    private boolean dirty;
    /**
     * 用于页面的锁
     */
    private Lock lock;

    /**
     * 保存了一个PageCache的引用，方便在拿到page的引用时可以快速对页面的缓存进行释放操作。
     * 页面缓存
     */
    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }


    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unLock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
