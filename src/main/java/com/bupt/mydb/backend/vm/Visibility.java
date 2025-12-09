package com.bupt.mydb.backend.vm;

import com.bupt.mydb.backend.tm.TransactionManager;

/**
 * @author gao98
 * date 2025/9/22 23:15
 * description:
 */
public class Visibility {

    /**
     * 对于读已提交隔离级别来说是允许如上版本跳跃问题的，但是对于可重复读是不允许的
     * 判断Entry对当前事务是否可见
     * 检查版本跳跃时，就是要取出要修改的数据x的最新提交版本，然后检查该最新版本的创建者对当前事务是否可见：
     * 如果是可重复读的隔离级别，同时检测到了版本跳跃，那么直接报错回滚
     * 判断是否可以跳过当前版本（用于MVCC版本链遍历优化）
     * 用于MVCC版本链遍历优化，判断是否可以跳过当前版本。
     *
     * @param tm 事务管理器
     * @param t  当前事务
     * @param e  待检查的Entry版本
     * @return 如果发生了版本跳跃返回true
     */
    //判断是否发生了MVCC中的版本跳跃问题
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        //获取删除该版本的事务id
        long xmax = e.getXmax();
        // 读已提交隔离级别来说是允许版本跳跃问题的
        if (t.level == 0) {
            return false;
        } else {
            //可重复读不允许版本跳跃，因此需要检测是否发生了版本跳跃
            //解决版本跳跃的思路：
            //如果事务a需要修改x，而x已经被a不可见的事务b修改了，那么要求a回滚
            //那么a不可见的事务b有哪些情况？具体是如下情况：
            //1,事务b的事务编号 > 事务a的事务编号（事务b在事务a之后才开启）
            //2,事务b 在事务a的开始前活跃事务集合内（b in SP(a)，即b在a开始之前已经开始，但a看不到b的修改）
            //检查版本跳跃时，就是要取出要修改的数据x的最新提交版本，
            //然后检查该最新版本的创建者对当前事务是否可见：
            //如果是可重复读的隔离级别，同时检测到了版本跳跃，那么直接报错回滚
            //xmax已提交：tm.isCommitted(xmax)。对当前事务不可见：xmax > t.xid：后开始的事务已修改，
            //t.isInSnapshot(xmax)：在事务快照中的活跃事务
            //检查删除该版本的事务 xmax是否已提交。
            //如果 xmax未提交，则该删除操作尚未生效，版本仍然可见（但这段代码是 isCommitted，所以它关注的是 已提交的删除）。
            //tm.isCommitted(xmax) 确保我们只关注 已提交的删除（未提交的删除不影响可见性）。
            //xmax > t.xid || t.isInSnapshot(xmax) 判断删除是否对 t有效：
            //xmax > t.xid → 删除发生在 t开始之后，对 t不可见。
            //t.isInSnapshot(xmax) → 删除事务在 t开始时未提交，对 t不可见。
            //返回值的意思是如果删除该版本的事务已经提交且删除事务id大于操作事务id，则返回true，表示该版本对当前事务不可见
            //亦或者删除该版本的事务已经提交且删除事务在操作事务开始时未提交，那么该版本对当前事务不可见，返回true，表示发生了版本跳跃
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * @param tm 事务管理器
     * @param t  当前事务
     * @param e  待检查的Entry
     * @return 如果可见返回true，否则返回false
     */
    // IsVisible 判断记录e是否对事务t可见
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        // 根据事务隔离级别选择不同的可见性判断逻辑
        // 如果隔离级别为读已提交级别
        if (t.level == 0) {
            //判断当前事务是否可以访问版本entry
            return readCommitted(tm, t, e);
        } else {
            //判断当前事务是否可以访问版本entry
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读已提交隔离级别的可见性判断
     * 读已提交级别意味着只能读取到其他事务已经提交的数据，可能导致不可重复读问题
     * SimpleDB中使用XMIN与XMAX来实现读已提交
     * XMIN：创建该版本的事务编号，当一个事务创建了一个新的版本后，XMIN会记录下来
     * XMAX：删除该版本的事务编号，当一个版本被删除或者有新版本出现时，XMAX会记录删除该版本的事务的编号
     * <p>
     * 基于读已提交（Read Committed）隔离级别的核心原则——事务只能读取其他事务已提交的数据，
     * 其可见性规则可归纳如下：
     * 一个数据版本对当前事务可见，当且仅当满足以下两个核心条件：第一，
     * 创建该版本的事务（XMIN）必须已经提交或是当前事务自身；第二，该版本必须未被删除（XMAX为空），
     * 或者删除操作是由一个尚未提交的事务（XMAX未提交）或由当前事务自身执行的。简而言之，
     * 事务无法看到由未提交事务创建的数据，但可以看到被未提交事务“标记”删除但尚未生效的数据。
     *
     * @param tm 事务管理器
     * @param t  当前事务
     * @param e  待检查的Entry
     * @return 如果可见返回true，否则返回false
     * <p>
     * readCommitted 如果是读已提交的隔离级别，判断e是否对事务t可见
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        // 获取事务的ID
        long xid = t.xid;
        // 获取记录的创建版本号
        long xmin = e.getXmin();
        // 获取记录的删除版本号
        long xmax = e.getXmax();

        // 如果记录的创建版本号等于事务的ID并且记录未被删除，则返回true
        // ---即记录e由当前事务创建且还未被删除
        if (xmin == xid && xmax == 0) {
            return true;
        }

        // 如果创建该记录的事务已提交
        if (tm.isCommitted(xmin)) {
            // 如果记录未被任何事务删除，则可见
            if (xmax == 0) {
                // ---这里代表的其实就是e由一个已提交的事务创建并且还未被删除
                return true;
            }
            // 如果xmax有值，说明xmax已经被删除，但是如果记录不是被当前事务删除的
            if (xmax != xid) {
                // 如果删除该记录的事务未提交，则记录对当前事务可见
                // 因为没有提交，代表该数据还是上一个版本可见的
                if (!tm.isCommitted(xmax)) {
                    // ---这里代表的是e由一个未提交的事务删除
                    return true;
                }
            }
        }
        // 其他情况均不可见
        return false;
    }

    /**
     * 可重复读隔离级别的可见性判断
     *
     * @param tm 事务管理器
     * @param t  当前事务
     * @param e  待检查的Entry
     * @return 如果可见返回true，否则返回false
     * <p>
     * 可重复读解决了不可重复读问题 （一直觉得这句话说了跟没说一样），但是仍然存在幻读问题。
     * 可重复读隔离级别下，事务只能读取它开始时已经提交了的事务产生的数据的版本。
     * 这意味着，在事务开始时已经提交的所有事务所产生的数据对当前事务是可见的，
     * 而在事务开始后产生的其他事务所产生的数据对当前事务则是不可见的。
     * 这样可以确保事务在执行期间读取到的数据是一致的，不会受到其他事务的影响。
     * <p>
     * <p>
     * <p>
     * 在可重复读（Repeatable Read）隔离级别下，一个数据版本对当前事务可见，
     * 当且仅当同时满足以下两个核心条件：第一，创建该版本的事务（XMIN）
     * 必须在当前事务开始时已提交（即不在事务快照中）或是当前事务自身；
     * 第二，该版本必须未被删除（XMAX为空），或者删除它的事务（XMAX）在当前事务开始时尚未提交
     * （即在事务快照中）或是当前事务自身。​​ 这套规则通过事务快照保证了在事务执行期间，
     * 其可见的数据范围是固定不变的，
     * 从而解决了不可重复读问题，即在同一事务内多次读取同一数据会得到相同的结果。
     * <p>
     * <p>
     * 如果版本的XMIN等于当前事务的事务编号，并且XMAX为空（表示尚未被删除），则该版本对当前事务可见（即该版本由当前事务创建并且还没被删除）
     * 如果版本的XMIN对应的事务已经提交，且XMIN小于当前事务的事务编号，
     * 并且XMIN不在当前的事务开始前活跃的事务集合中（SP(Ti),Ti为当前事务）且满足下列条件之一：
     * XMAX为空（该版本尚未被删除）
     * XMAX不是当前事务的事务编号，并且XMAX对应的事务尚未提交，并且XMAX大于当前事务的事务编号（由其他事务删除，但是删除他的事务尚未提交或者这个事务在当前事务之后才开始）
     * XMAX在当前事务开始前活跃的事务集合中（SP(Ti)）
     */
// 如果是可重复读的隔离级别，判断e是否对事务t可见
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        // 获取事务的ID
        long xid = t.xid;
        // 获取记录的创建版本号
        long xmin = e.getXmin();
        // 获取记录的删除版本号
        long xmax = e.getXmax();

        // 如果记录由当前事务创建且未被删除，则可见
        // ---即记录e由当前事务创建且还未被删除
        if (xmin == xid && xmax == 0) {
            return true;
        }

        // 如果记录e的创建版本已经提交，并且创建版本号小于事务的ID，并且创建版本号不在事务的快照中
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 如果记录未被任何事务删除，则可见
            if (xmax == 0) {
                return true;
            }
            // 如果条目的删除版本号不等于事务的ID
            if (xmax != xid) {
                // 如果条目的删除版本未提交，
                // 或者删除版本号大于事务的ID，
                // 或者删除版本号在事务的快照中，则返回true
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        // 其他情况均不可见
        return false;
    }
}