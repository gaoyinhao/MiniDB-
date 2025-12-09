package com.bupt.mydb.backend.dm.page;

/**
 * @author gao98
 * date 2025/9/17 21:47
 * description:
 * ## 页面缓存
 * 数据库中实现页面缓存的相关设计和实现。
 * 为了方便管理，数据库中的各种数据（日志除外）都存储在一个.db文件中，后面称为db文件。
 * 而为了更方便地存取数据以及缓存数据，数据管理器（即DM）在操作db文件时是以页面为单位的。
 * 因此，这里需要先规定页面的相关定义。
 */

public interface Page {
    // 上锁
    void lock();

    //解锁
    void unLock();

    //释放页面资源
    void release();

    //标记页面是否为脏页
    void setDirty(boolean dirty);

    //检查页面是否为脏页
    boolean isDirty();

    //获取页面唯一标识号
    int getPageNumber();

    //获取页面原始数据
    byte[] getData();
}
