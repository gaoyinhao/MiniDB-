package com.bupt.mydb.backend.vm;

import com.bupt.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gao98
 * date 2025/9/22 23:15
 * description:
 * vm对一个事务的抽象
 */
public class Transaction {
    public long xid;
    //事务隔离级别
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /**
     * 创建一个新的事务对象
     * 
     * @param xid 事务ID
     * @param level 事务隔离级别
     * @param active 当前活跃的事务集合
     * @return 新创建的事务对象
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 只有在隔离级别不为0时才创建快照
        if (level != 0) {
            t.snapshot = new HashMap<>();
            // 将当前所有活跃事务添加到快照中
            for (Long activeXid : active.keySet()) {
                t.snapshot.put(activeXid, true);
            }
        }
        return t;
    }

    /**
     * 检查指定的事务ID是否在当前事务的快照中
     * 
     * @param xid 要检查的事务ID
     * @return 如果在快照中返回true，否则返回false
     */
    public boolean isInSnapshot(long xid) {
        // 超级事务不在快照检查范围内
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        // 检查快照中是否包含指定的事务ID
        return snapshot != null && snapshot.containsKey(xid);
    }
}
