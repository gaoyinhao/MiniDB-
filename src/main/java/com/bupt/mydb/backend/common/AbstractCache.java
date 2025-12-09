package com.bupt.mydb.backend.common;

import com.bupt.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/17 22:56
 * description:
 * 通用缓存框架AbstracCache
 * <p>
 * 这是一个实现了引用计数策略的通用缓存抽象类，主要用于管理内存中的缓存资源。
 * 实现缓存框架时要考虑的一个点是缓存策略，这里采用的策略是使用引用计数。
 * 当某个资源被使用到时，加入到缓存，如果有其他的模块也在访问这个缓存，那么计数就加1，
 * 只有当所有的模块都释放了该资源时，将其从缓存中释放。
 * 使用循环确保最终能获取到资源
 * <p>
 * 主要功能
 * 缓存管理：维护一个键值对缓存（使用HashMap实现）
 * <p>
 * 引用计数：跟踪每个缓存项的引用次数
 * <p>
 * 线程安全：使用锁机制确保多线程环境下的安全访问
 * <p>
 * 资源限制：可以设置最大缓存资源数
 * <p>
 * 资源加载/释放：通过抽象方法实现具体资源的加载和释放
 */
public abstract class AbstractCache<T> {
    //存储缓存数据的HashMap
    private HashMap<Long, T> cache;
    //记录每个缓存项的引用计数
    private HashMap<Long, Integer> references;
    //标记正在被获取的资源，防止重复加载
    //记录哪些资源当前正在从数据源中获取，key是资源的唯一标识符uid，value是布尔值，
    // 表示该资源是否正在被获取中。需要这个map的原因是多线程环境下可能一个资源被同时获取，
    // 而获取的过程较长（如从文件中读取），防止多线程并发问题而引入该map
    private HashMap<Long, Boolean> getting;
    //缓存的最大资源数，防止内存溢出
    private int maxResource;
    //当前缓存中的项目数
    private int count;
    //用于线程同步的锁
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * * 处理三种情况：
     * * 资源正在被其他线程获取：短暂等待后重试
     * * 资源在缓存中：增加引用计数并返回
     * * 资源不在缓存中：尝试加载新资源
     * <p>
     * Get的基本流程就是进入死循环，不断尝试从缓存中获取。
     * 首先要判断是否有其他线程正在从数据源中获取这个资源，如果有的话就待会再看，否则：
     * <p>
     * 如果资源已经在缓存中，那么就直接获取并返回，同时给资源的引用数+1
     * 如果资源不在缓存中，那么就在getting中标记，然后该线程从数据源中获取资源
     * <p>
     * 可以看到代码在获取资源时，调用了GetForCache，这里就是实际的缓存实现来完成逻辑
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        //表示会不断尝试直到成功获取资源
        while (true) {
            //获取锁保证线程安全
            lock.lock();
            //判断是否有其他线程正在获取资源
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取，释放锁
                lock.unlock();
                try {
                    //睡眠1毫秒（减少忙等待的CPU消耗）
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                //继续循环重试
                continue;
            }
            //处理缓存命中的情况
            if (cache.containsKey(key)) {
                //资源在缓存中，获取缓存对象
                T obj = cache.get(key);
                //增加该对象的引用计数
                references.put(key, references.get(key) + 1);
                //释放锁
                lock.unlock();
                //返回对象
                return obj;
            }
            // 缓存中现在没有该资源
            // 尝试获取该资源,检查资源限制,
            // 如果有资源数量限制(maxResource > 0)且已达上限,释放锁并抛出缓存已满异常
            // 缓存已满，需要删除一个资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CACHE_FULL_EXCEPTION;
            }
            //准备获取资源
            //增加缓存计数器
            count++;
            //在getting集合中标记该key正在被获取
            getting.put(key, true);
            //释放锁
            lock.unlock();
            //跳出循环（进入实际获取阶段）
            break;
        }
        //实际获取资源
        T obj = null;
        try {
            //尝试调用getForCache（从底层加载资源）
            //可以代码在获取资源时，调用了GetForCache，这里就是实际的缓存实现来完成逻辑
            obj = getForCache(key);
        } catch (Exception e) {
            //如果失败：1，获取锁；2，恢复计数器；3，清除getting标记；4，释放锁；5，抛出异常；
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //从底层加载资源成功
        //缓存新获取到的资源
        //获取锁
        lock.lock();
        //清除getting标记
        getting.remove(key);
        //将新对象放入缓存
        cache.put(key, obj);
        //设置引用计数为1
        references.put(key, 1);
        //释放锁
        lock.unlock();
        //返回对象
        return obj;
    }

    /**
     * 强行释放一个缓存
     * 释放缓存时，将对应的引用计数-1，如果已经减到0了就回源，然后删除缓存中相关的各种数据
     *
     * @param key
     * @throws Exception
     */
    protected void release(long key) {
        //获取锁
        lock.lock();
        try {
            //将该缓存的引用计数减一
            int ref = references.get(key) - 1;
            //如果引用计数已经为0，则从缓存中移除
            if (ref == 0) {
                //拿到该缓存
                T obj = cache.get(key);
                // 看该页是否为脏页，是的话就写回到磁盘中。
                releaseForCache(obj);
                //从引用Map中移除该key
                references.remove(key);
                //从缓存Map中移除该key
                cache.remove(key);
                //总的缓存数量减一
                count--;
            } else {
                //更新引用计数
                references.put(key, ref);
            }
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     * 在关闭缓存结构时，所有的缓存资源要强行回源
     */
    protected void close() {
        //加锁
        lock.lock();
        try {
            //获取所有的缓存keySet
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                //根据key拿到value
                T obj = cache.get(key);
                //释放该资源
                releaseForCache(obj);
                //移除该缓存的引用计数
                references.remove(key);
                //从缓存map中移除
                cache.remove(key);
            }
        } finally {
            //解锁
            lock.unlock();
        }
    }


    /**
     * 子类必须实现，定义如何从底层加载资源
     * <p>
     * GetForCache是实际用来获取数据并将其放入到缓存的函数。
     * 对应着资源不存在于缓存中的获取行为
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 子类必须实现，定义如何释放/写回资源
     * <p>
     * ReleaseForCache是释放指定的缓存数据，如果此时该数据的应用计数为0，
     * 则将其从缓存中驱逐出去。对应着资源被驱逐时的写回行为
     *
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
