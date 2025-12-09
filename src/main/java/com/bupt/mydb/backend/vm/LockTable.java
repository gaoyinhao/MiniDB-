package com.bupt.mydb.backend.vm;

import com.bupt.mydb.common.Error;
import org.checkerframework.checker.units.qual.A;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/22 21:19
 * description: 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    // 某个XID已经获得的资源的UID列表，键是事务ID，值是该事务持有的资源ID列表。
    private Map<Long, List<Long>> x2u;
    // UID被某个XID持有,键是资源ID，值是持有该资源的事务ID。
    private Map<Long, Long> u2x;
    // 正在等待UID的XID列表，键是资源ID，值是正在等待该资源的事务ID。
    private Map<Long, List<Long>> wait;
    // 正在等待资源的XID的锁,键是事务ID，值是该事务的锁对象。
    private Map<Long, Lock> waitLock;
    // XID正在等待的UID,键是事务ID，值是该事务正在等待的资源ID。
    private Map<Long, Long> waitU;
    //保护LockTable内部状态的锁，确保线程安全
    private Lock lock;

    // 以下两个字段用于死锁检测
    //一个Map集合，用于存储事务ID(Long类型)与时间戳(Integer类型)的映射关系
    //主要用于事务的时间戳管理，xidStamp记录每个事务对应的时间戳
    //在死锁检测中用于标记访问状态
    // xidStamp 事务ID的时间戳映射
    private Map<Long, Integer> xidStamp;
    //全局时间戳计数器,用于生成递增的时间戳值
    //每次检测时递增，确保每次检测使用唯一的时间戳
    // stamp 全局的时间戳
    private int stamp;


    /**
     * 初始化所有数据结构
     */
    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 不需要等待则返回null，否则返回锁对象
     * 会造成死锁则抛出异常
     * 添加锁请求
     * 在每次由于获取锁而出现阻塞的情况时，就尝试向依赖等待图中增加一个有向边，
     * 然后进行死锁检测，如果发生死锁，那么返回错误并撤销异常
     *
     * @param xid 事务id
     * @param uid 资源id
     * @return
     * @throws Exception
     */
    // Add 添加一个事务ID和资源ID的映射关系，返回一个锁对象，如果发生死锁，返回错误
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            //检查事务XID是否已经持有该资源，如果是则直接返回null
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            // 检查UID资源是否已经被其他XID事务持有
            if (!u2x.containsKey(uid)) {
                // 如果没有被持有，将资源分配给当前事务
                u2x.put(uid, xid);
                // 将资源添加到事务的资源列表中
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 如果资源已经被其他事务持有，将当前事务添加到等待列表中(waitU是一个map，
            // 键是事务ID，值是资源ID，代表事务正在等待的资源
            waitU.put(xid, uid);
            // 反过来，将资源添加到等待列表中(wait是一个map，键是资源ID，值是事务ID列表，代表正在等待该资源的事务)
            putIntoList(wait, uid, xid);
            //检查是否会产生死锁，如果会则抛出异常
            if (hasDeadLock()) {
                // 如果存在死锁，从等待列表中移除当前事务
                waitU.remove(xid);
                // 从资源的等待列表中移除当前事务
                removeFromList(wait, uid, xid);
                throw Error.DEADLOCK_EXCEPTION;
            }
            // 否则创建并返回一个新的锁对象
            // 如果不存在死锁，为当前事务创建一个新的锁，并锁定它
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    // Remove 当一个事务commit或者abort时，就会释放掉它自己持有的资源锁，并将自身从等待图中删除
    public void remove(long xid) {
        lock.lock();
        try {
            //获取指定事务xid持有的所有资源UID列表
            List<Long> l = x2u.get(xid);
            if (l != null) {
                //遍历并逐个释放这些资源，并为每个资源选择新的等待事务来占用；
                while (!l.isEmpty()) {
                    // 获取并移除列表中的第一个资源ID
                    Long uid = l.remove(0);
                    // 从等待队列中选择一个新的事务ID来占用这个资源
                    selectNewXID(uid);
                }
            }
            // 从waitU映射中移除当前事务ID
            waitU.remove(xid);
            // 从x2u映射中移除当前事务ID
            x2u.remove(xid);
            // 从waitLock映射中移除当前事务ID
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }


    /**
     * 从等待队列中选择一个事务xid来占用资源uid
     * 当一个事务释放资源时，从等待队列中选择一个事务来占用该资源
     *
     * @param uid
     */
    private void selectNewXID(long uid) {
        //移除资源UID的当前持有者XID
        //从 u2x 映射中移除该资源的持有者记录。u2x 是一个 Map，键是资源ID，值是持有该资源的事务ID。
        u2x.remove(uid);
        //找出等待UID的XID集合，从 wait 映射中获取等待该资源的事务列表。wait 是一个 Map，键是资源ID，值是等待该资源的事务ID列表
        // 从等待队列中获取当前资源ID的等待列表
        List<Long> l = wait.get(uid);
        //如果等于null直接返回，如果size小于0直接报错，如果没有事务等待该资源，则直接返回。同时断言确保如果有列表，则列表不为空
        // 如果等待队列为空，立即返回
        if (l == null) {
            return;
        }
        // 断言等待队列不为空
        assert !l.isEmpty();

        // 遍历等待队列
        //如果size>0，就取出第一个值，遍历等待队列，选择合适的事务
        while (!l.isEmpty()) {
            // 获取并移除队列中的第一个事务ID
            // 从等待列表的第一个事务开始处理（FIFO策略）
            long xid = l.remove(0);
            //检查该事务是否还在等待（通过 waitLock 映射确认）
            //如果事务已经不在等待状态，则继续检查下一个事务
            // 检查事务ID是否在waitLock映射中
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                //如果在waitLock映射中，表示这个事务ID已经被锁定
                //如果事务仍在等待，则
                //将资源分配给该事务：u2x.put(uid, xid)
                // 将事务ID和资源ID添加到u2x映射中
                u2x.put(uid, xid);
                //从等待锁映射中移除该事务的锁：waitLock.remove(xid)
                // 从waitLock映射中移除这个事务ID
                Lock lo = waitLock.remove(xid);
                //从等待资源映射中移除该事务的等待记录：waitU.remove(xid)
                // 从waitU映射中移除这个事务ID
                waitU.remove(xid);
                //解锁该事务的等待锁，使其可以继续执行：lo.unlock()
                // 解锁这个事务ID的锁
                lo.unlock();
                //跳出循环，处理结束
                break;
            }
        }
        if (l.isEmpty()) {
            wait.remove(uid);
        }
    }


    /**
     * 死锁检测主方法
     *
     * @return
     */
    // hasDeadLock 检查是否存在死锁
    private boolean hasDeadLock() {
        // 初始化: 创建新的 xidStamp 映射和重置 stamp 计数器
        // 创建一个新的xidStamp哈希映射
        xidStamp = new HashMap<>();
        // 将stamp设置为1
        stamp = 1;
        //遍历事务: 遍历所有当前持有资源的事务 (x2u.keySet())
        // 遍历所有已经获得资源的事务ID
        for (long xid : x2u.keySet()) {
            // 跳过已处理事务: 如果事务已被标记且时间戳大于0，说明已在之前的DFS中处理过，跳过
            // 获取xidStamp中对应事务ID的记录
            Integer s = xidStamp.get(xid);
            // 如果记录存在，并且值大于0,说明已在之前的DFS中处理过，跳过
            if (s != null && s > 0) {
                // 跳过这个事务ID，继续下一个
                continue;
            }
            //递增时间戳: 增加全局时间戳 stamp，为本次DFS标记使用
            stamp++;
            //执行DFS检测: 对当前事务执行深度优先搜索
            if (dfs(xid)) {
                //返回结果: 如果发现死锁返回true，否则继续检查下一个事务
                // 如果dfs方法返回true，表示存在死锁，那么hasDeadLock方法也返回true
                return true;
            }
        }
        // 如果所有的事务ID都被检查过，并且没有发现死锁，那么hasDeadLock方法返回false
        return false;
    }

    /**
     * 深度优先搜索: 递归遍历事务的等待链，检查是否存在环路
     * 由于2PL的存在，某些事务在获取某些Entry的锁时，
     * 可能会阻塞，那么很容易地就能知道这种依赖资源形成的阻塞关系会形成一个有向图，
     * 图中的节点是事务，边代表事务的依赖关系，那么就可能出现死锁。
     * @param xid
     * @return
     */
    /**
     * 例如，现在访问xid1，xid1需要访问资源A，但资源A通过u2x得到被xid2所访问，因此dfsxid2，xid2需要访问资源B，
     * 但是资源B通过u2x发现正在被xid1所访问，则dfs xid1，xidStamp已经标记过xid1，因此返回true
     * <p>
     * 在每次由于获取锁而出现阻塞的情况时，就尝试向依赖等待图中增加一个有向边，
     * 然后进行死锁检测，如果发生死锁，那么返回错误并撤销异常
     *
     * @param xid
     * @return
     */
    private boolean dfs(long xid) {
        //获取事务时间戳: 从 xidStamp 中获取当前事务的时间戳
        // 从xidStamp映射中获取当前事务ID的时间戳
        Integer stp = xidStamp.get(xid);
        //检测环路:如果时间戳stp等于当前 stamp，说明在当前DFS路径中已访问过该事务，形成环路，返回true
        // 如果时间戳存在并且等于全局时间戳，说明发生了死锁（这意味着是在dfs递归的过程中又检查到了这个xid）
        if (stp != null && stp == stamp) {
            return true;
        }
        //如果时间戳小于当前 stamp，说明该事务在之前DFS中已处理过且无环路，返回false
        // 如果时间戳存在并且小于全局时间戳
        if (stp != null && stp < stamp) {
            // 这个事务ID已经被检查过，并且没有发现死锁，返回false
            return false;
        }
        //标记访问: 将当前事务标记为已访问 (xidStamp.put(xid, stamp))
        // 如果时间戳不存在，将当前事务ID的时间戳设置为全局时间戳
        xidStamp.put(xid, stamp);
        // 从waitU映射中获取当前事务ID正在等待的资源ID
        //获取等待资源: 从 waitU 中获取事务正在等待的资源
        Long uid = waitU.get(xid);
        //检查等待关系: 如果事务没有等待任何资源，返回false
        if (uid == null) {
            // 如果资源ID不存在，表示当前事务ID不在等待任何资源，返回false
            return false;
        }
        //获取资源持有者: 从 u2x 中获取持有该资源的事务
        // 从u2x映射中获取当前资源ID被哪个事务ID持有，这里应该是一定有的，
        // 因为在Add方法中，如果资源ID存在，一定会被分配给一个事务ID
        Long x = u2x.get(uid);
        assert x != null;
        //递归检测: 对资源持有者事务递归执行DFS
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) {
            return;
        }
        Iterator<Long> iterator = l.iterator();
        while (iterator.hasNext()) {
            long e = iterator.next();
            if (e == uid1) {
                iterator.remove();
                break;
            }
        }
        if (l.isEmpty()) {
            listMap.remove(uid0);
        }
    }


    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }


    // isInList 给定事务xid和资源uid，判断当前事务是否持有该资源，如果已经持有，返回true，否则返回false
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        // 先获取事务xid持有的资源列表
        List<Long> l = listMap.get(uid0);
        if (l == null) {
            return false;
        }
        Iterator<Long> i = l.iterator();
        // 遍历资源列表，如果找到了资源uid，返回true
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                return true;
            }
        }
        return false;
    }

}
