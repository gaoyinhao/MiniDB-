package com.bupt.mydb.backend.dm.pageIndex;

import com.bupt.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/19 22:14
 * description:
 * PageIndex是数据库系统中页面空间索引的高效实现，通过区间划分和锁机制来管理页面空闲空间。
 * <p>
 * 前文已经完成了对页面，即Page的定义，数据库的各种数据存储在Page中
 * 目前的实现中，一个Page的大小是固定的，而数据会插入在仍然有空闲空间的Page中
 * 这带来了一个问题，我们怎么知道哪个Page空闲的空间中能够存储当前要插入的数据呢？如果只是简单的从第一页开始遍历所有Page去检查，那是不是效率很低？而如果随机查找的话会不会又引入空间的碎片？（类似内存或磁盘的碎片）
 * 为此，要引入简单的页面索引机制，用于保存当前各个Page的空闲空间
 * <p>
 * <p>
 * <p>
 * 页面索引的作用：
 * 页面索引的设计用于提高在数据库中进行插入操作时的效率。它缓存了每一页的空闲空间信息，
 * 以便在进行插入操作时能够快速找到合适的页面，而无需遍历磁盘或者缓存中的所有页面
 * <p>
 * <p>
 * 页面索引的实现思路
 * 将每一个Page划分为一定数量的区间（这里选择的是40个区间）
 * 启动数据库时，遍历所有的Page，将每个Page的空闲空间信息分配到这些区间中
 * 当需要插入数据时，首先会将所需的空间大小向上取整，然后映射到某个区间，随后可以直接从该区间中选择任何一页来满足插入需求
 * <p>
 * PageIndex
 * PageIndex是一个数组，数组的每一个元素是一个列表，用于存储具有相同空闲空间大小的页面信息。也就是说，数组中同一个下标中的列表存储的是同样空闲大小的那些页面，下标为i的数组元素中的列表中都是空闲大小为PageSize/40 * i的页面
 * 从PageIndex中获取页面的过程比较简单，只需要根据所需的空间大小计算得出需要的下标，然后从列表中取出来一个页面即可
 * 被选择的页面从PageIndex中移除，这意味着一个Page不允许被并发写入。上层模块使用完页面后，需要将这个页面重新放回PageIndex中，以便其他操作申请页面时使用
 */


public class PageIndex {
    //将一页划分成40个区间
    // IntervalsNumber 将一页划分为40个区间
    private static final int INTERVALS_NO = 40;
    //每个区间大小：THRESHOLD = PAGE_SIZE / 40,例如：8KB页→8192/40
    // IntervalSize 每个区间大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    //区间页面列表数组
    // lists ，数组表示的是区间集合，集合存的是区间内的页
    private List<PageInfo>[] lists;

    /**
     * 区间分配：lists[0]~lists[40]
     * 溢出处理：lists[40]存储超大空间页面
     */
    public PageIndex() {
        lock = new ReentrantLock();
        // 41个区间(含溢出区)
        //因为一个页面最多40个区，从0到40个区，数组中下标为0的集合意思是该集合中的页面还有0个空闲区，下标为1意思是该集合中的页面还有1个空闲区，以此类推
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 添加页面
     * 将一个空闲页面的信息重新添加回页面索引
     *
     * @param pgno
     * @param freeSpace eg：
     *                  freeSpace=250 → number=2 (当THRESHOLD=100)，添加到lists[2]
     */
    // Add 添加页的索引信息
    // 根据给定的页面编号和空闲空间大小添加一个 PageInfo 对象
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 计算区间数，区间计算：freeSpace / THRESHOLD
            // 使用页的剩余空间计算应该添加哪个索引
            // 计算空闲空间大小对应的区间数
            int number = freeSpace / THRESHOLD;
            // 在对应的区间列表中添加一个新的 PageInfo 对象
            lists[number].add(new PageInfo(pgno, number));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 选择页面，根据数据大小智能选择最适合的数据库页面。
     *
     * @param spaceSize 需要插入的数据大小（字节）
     * @return 包含页面信息的PageInfo对象，若无合适页面返回null
     * <p>
     * 数据大小：350字节
     * PageInfo pi = pageIndex.select(350);
     * 查找过程：
     * 1. 350/100=3 → 检查区间4(400-499)
     * 2. 找到页面 → 返回PageInfo
     * <p>
     * 数据大小：420字节
     * PageInfo pi = pageIndex.select(420);
     * 查找过程：
     * 1. 420/100=4 → 检查区间5(500-599)
     * 2. 区间5空 → 检查区间6(600-699)
     * 3. 找到页面 → 返回PageInfo
     */
    // Select 选择一个页
    // 根据给定的空间大小选择一个有合适空间的页面，包装成一个PageInfo对象
    // 返回一个 PageInfo 对象，其空闲空间>=所需的空间大小。如果没有找到合适的 PageInfo，返回 null
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            //区间计算：spaceSize / THRESHOLD，例如：数据350字节，THRESHOLD=100→ 350/100=3
            // 计算所需的空间大小等于多少个区间
            int number = spaceSize / THRESHOLD;
            // 向上取整，number++确保找到足够大的区间，3 → 4（查找400字节区间）
            // 如果计算出的区间编号小于总的区间数，编号加一 , 此处+1主要为了向上取整
            if (number < INTERVALS_NO) {
                number++;
            }
            // 从计算出的区间编号开始，向上寻找合适的 PageInfo
            while (number <= INTERVALS_NO) {
                // 如果当前区间没有 PageInfo，继续查找下一个区间
                if (lists[number].isEmpty()) {
                    number++;
                    continue;
                }
                // FIFO策略
                return lists[number].remove(0);
            }
            // 如果没有找到合适的 PageInfo，返回 nil
            return null;
        } finally {
            lock.unlock();
        }
    }

}
