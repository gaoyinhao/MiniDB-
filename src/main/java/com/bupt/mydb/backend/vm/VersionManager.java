package com.bupt.mydb.backend.vm;

import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.tm.TransactionManager;

/**
 * @author gao98
 * date 2025/9/22 23:15
 * description:

 */
public interface VersionManager {
    /**
     * 读取指定事务可见的数据项
     *
     * @param xid 事务ID
     * @param uid 数据项的唯一标识符
     * @return 数据项的字节数组表示
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    byte[] read(long xid, long uid) throws Exception;

    /**
     * 在指定事务中插入新数据
     *
     * @param xid  事务ID
     * @param data 要插入的字节数据
     * @return 新数据项的UID
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 在指定事务中删除数据项
     *
     * @param xid 事务ID
     * @param uid 要删除的数据项的唯一标识符
     * @return 如果成功删除返回true，否则返回false
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    boolean delete(long xid, long uid) throws Exception;

    /**
     * 开启一个新的事务
     *
     * @param level 事务的隔离级别
     * @return 新事务的ID
     */
    long begin(int level);

    /**
     * 提交指定的事务
     *
     * @param xid 要提交的事务ID
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    void commit(long xid) throws Exception;

    /**
     * 回滚指定的事务
     *
     * @param xid 要回滚的事务ID
     */
    void abort(long xid);

    /**
     * 创建一个新的版本管理器实例
     *
     * @param transactionManager 事务管理器
     * @param dataManager        数据管理器
     * @return VersionManager实例
     */
    static VersionManager newVersionManager(TransactionManager transactionManager, DataManager dataManager) {
        return new VersionManagerImpl(transactionManager, dataManager);
    }
}
