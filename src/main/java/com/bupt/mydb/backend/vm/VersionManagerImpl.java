package com.bupt.mydb.backend.vm;

import com.bupt.mydb.backend.common.AbstractCache;
import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.tm.TransactionManager;
import com.bupt.mydb.backend.tm.TransactionManagerImpl;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/22 23:15
 * description:
 * 负责处理多版本并发控制(MVCC)和事务隔离。
 * VM层通过VersionManager，向上层提供api接口以及各种功能，
 * 对于VM上层的模块（使用了VM层接口的上层模块），操作的都是Entry结构
 * 而VM依赖于DM，所以VM视角里，操作的是DataItem
 */

//    可以看到其中有一个CacheManager，说明也是实现了抽象缓存的，
//    用到了模板方法设计模式，这个在介绍日志模块和数据管理器时也见到过
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    // 事务管理器，用于管理事务的创建、提交、回滚等操作
    TransactionManager tm;
    // 数据管理器，用于管理数据的读取和写入操作
    DataManager dm;
    // 活跃事务映射表，保存所有当前活跃的事务对象
    Map<Long, Transaction> activateTransaction;
    // 全局锁，用于保护对活跃事务映射表的并发访问
    Lock lock;
    // 锁表，用于维护事务间的锁关系和死锁检测
    LockTable lt;

    //  创建一个新的VersionManager
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activateTransaction = new HashMap<>();
        activateTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        // 核心还是调用dm.Read()方法
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NULL_ENTRY_EXCEPTION;
        }
        return entry;
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }


    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    // Read 读取一个entry，需要判断可见性
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activateTransaction.get(xid);
        lock.unlock();
        // 如果事务为空或者已经出错，那么抛出错误
        if (t == null) {
            throw Error.NO_TRANSACTION_EXCEPTION;
        }
        if (t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NULL_ENTRY_EXCEPTION) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            // 判断这个entry对当前事务是否可见
            // 如果数据项对当前事务可见，那么返回数据项的数据
            if (Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            // 释放数据项
            entry.release();
        }
    }

    // Insert 将数据包裹成Entry，然后交给DM插入即可
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activateTransaction.get(xid);
        lock.unlock();

        if (t == null) {
            throw Error.NO_TRANSACTION_EXCEPTION;
        }
        // 如果事务已经出错，那么抛出错误
        if (t.err != null) {
            throw t.err;
        }
        // 将事务ID和数据包装成一个新的数据项
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        // 调用数据管理器的insert方法，插入新的数据项，并返回数据项的唯一标识符
        return dm.insert(xid, raw);
    }

    // Delete 删除一个数据项
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activateTransaction.get(xid);
        lock.unlock();

        if (t == null) {
            throw Error.NO_TRANSACTION_EXCEPTION;
        }
        // 如果事务已经出错，那么抛出错误
        if (t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == Error.NULL_ENTRY_EXCEPTION) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            // 如果数据项对当前事务不可见，那么返回false
            if (!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                // 尝试为数据项添加锁
                l = lt.add(xid, uid);
            } catch (Exception e) {
                // 如果出现并发更新的错误，那么中止事务，并抛出错误
                t.err = Error.CONCURRENT_UPDATE_EXCEPTION;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 如果成功获取到锁，那么锁定并立即解锁
            if (l != null) {
                l.lock();
                l.unlock();
            }
            // 如果数据项已经被当前事务删除，那么返回false
            if (entry.getXmax() == xid) {
                return false;
            }
            // 如果数据项的版本被跳过，那么中止事务，并抛出错误
            // 如果返回true，表面该数据的该版本对当前事务不可见
            if (Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.CONCURRENT_UPDATE_EXCEPTION;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            // 设置数据项的xmax为当前事务的ID，表示数据项被当前事务删除
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    // Begin 开启一个事务，并初始化事务的结构
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            // 调用事务管理器的begin方法，开始一个新的事务，并获取事务ID
            long xid = tm.begin();
            // 创建一个新的事务对象
            Transaction t = Transaction.newTransaction(xid, level, activateTransaction);
            // 将事务对象添加到活动事务中
            activateTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    //主要就是 free 掉相关的结构，并且释放持有的锁，并修改事务状态
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activateTransaction.get(xid);
        lock.unlock();

        if (t == null) {
            throw Error.NO_TRANSACTION_EXCEPTION;
        }

        try {
            // 如果事务已经出错，那么抛出错误
            if (t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException n) {
            System.out.println(xid);
            System.out.println(activateTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        // 从活动事务中移除这个事务
        activateTransaction.remove(xid);
        lock.unlock();

        // 从锁表中移除这个事务的锁
        lt.remove(xid);
        // 调用事务管理器的commit方法，进行事务的提交操作
        tm.commit(xid);
    }

    /**
     * 中止事务的方法则有两种，手动和自动。手动指的是调用 abort() 方法，
     * 而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
     *
     * @param xid 要回滚的事务ID
     */
    // Abort 公开的abort方法，用于中止一个事务，手动停止事务的执行
    @Override
    public void abort(long xid) {
        // 调用内部的abort方法，autoAborted参数为false表示这不是一个自动中止的事务
        internAbort(xid, false);
    }

    // abort事务的方法有两种:手动和自动
    // 手动指的是调用abort()方法
    // 自动是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚
    // internAbort 内部的abort方法，处理事务的中止
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activateTransaction.get(xid);
        // 如果这不是一个自动中止的事务，那么从活动事务中移除这个事务
        if (!autoAborted) {
            activateTransaction.remove(xid);
        }
        lock.unlock();
        // 如果事务已经被自动中止，那么直接返回，不做任何处理
        if (t == null || t.autoAborted) {
            return;
        }
        // 从锁表中移除这个事务的锁
        lt.remove(xid);
        // 调用事务管理器的abort方法，进行事务的中止操作
        tm.abort(xid);
    }
}
