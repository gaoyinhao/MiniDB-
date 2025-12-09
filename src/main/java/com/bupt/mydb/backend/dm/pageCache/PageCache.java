package com.bupt.mydb.backend.dm.pageCache;

import com.bupt.mydb.backend.dm.page.Page;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author gao98
 * date 2025/9/17 22:00
 * description:
 * 页面缓存定义了各种操作Page的接口：创建、获取、释放页面等。
 * 其需要实现抽象缓存AbstractCache
 * 这里注意，pageNumbers对应的是操作的页数，考虑到并发情况，这应该是原子操作的，后面会提到
 */
public interface PageCache {
    /**
     * 8192
     * 8KB
     * 页面大小
     */
    int PAGE_SIZE = 1 << 13;

    /**
     * 新建页面
     *
     * @param initData
     * @return
     */
    int newPage(byte[] initData);

    /**
     * 获取页面
     *
     * @param pgno
     * @return
     * @throws Exception
     */
    Page getPage(int pgno) throws Exception;

    /**
     * 关闭缓存
     */
    void close();

    /**
     * 释放页面
     *
     * @param page
     */
    void release(Page page);

    /**
     * 根据最大页号截断缓存
     *
     * @param maxPgno
     */
    void truncateByPgno(int maxPgno);

    /**
     * 获取当前页面数量
     *
     * @return
     */
    int getPageNumber();

    /**
     * 刷新页面
     *
     * @param pg
     */
    void flushPage(Page pg);


    /**
     * CreatePageCache 创建页面缓存
     * 也就是将db文件加载到内存中，然后在按页大小分隔为多个页面，缓存起来， 然后返回PageCache实例
     *
     * @param path
     * @param memory memory参数限制内存使用量
     * @return
     */
    static PageCacheImpl create(String path, long memory) {
        //创建带有.db后缀的数据库文件对象
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            //原子性地创建新文件,如果文件已存在则panic
            if (!f.createNewFile()) {
                Panic.panic(Error.FILE_EXISTS_EXCEPTION);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        //确保文件可读写,防止后续操作失败
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        //使用随机访问模式
        RandomAccessFile randomAccessFile = null;
        //获取NIO文件通道提高性能
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        //计算最大缓存页数：memory / PAGE_SIZE,初始化PageCacheImpl实例
        // 设置内存为多大，然后除以一页的大小，即得到一个db文件可以分几页
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }

    /**
     * OpenPageCache 打开页面缓存
     *
     * @param path
     * @param memory
     * @return
     */
    static PageCacheImpl open(String path, long memory) {
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        //确保数据库文件存在,不存在则立即失败
        if (!file.exists()) {
            Panic.panic(Error.FILE_NOT_EXISTS_EXCEPTION);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, fileChannel, (int) memory / PAGE_SIZE);
    }
}
