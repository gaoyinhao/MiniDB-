package com.bupt.mydb.backend.dm.pageCache;

import com.bupt.mydb.backend.common.AbstractCache;
import com.bupt.mydb.backend.dm.page.Page;
import com.bupt.mydb.backend.dm.page.PageImpl;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/18 10:06
 * description:
 * 这是数据库系统的页面缓存实现，负责管理数据库文件中的页面（数据块）在内存中的缓存。
 * 是数据库系统中负责管理数据库页面缓存的核心组件
 * <p>
 * 页面缓存管理：缓存最近使用的数据库页面，减少磁盘I/O
 * 页面读写：从文件读取页面/将页面写入文件，管理新页面的创建和分配
 * 页面分配：管理新页面的创建
 * 文件管理：处理数据库文件的打开，关闭和截断
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    // 最小内存页数限制
    private static final int MEN_MIN_LIM = 10;
    // 数据库文件后缀
    public static final String DB_SUFFIX = ".db";
    // 数据库文件
    private RandomAccessFile file;
    // NIO文件通道（提供高效文件操作）
    private FileChannel fileChannel;
    // 文件操作锁（保证线程安全）
    // 可重入锁
    private Lock lock;
    // 当前数据库的页面总数
    // pageNumbers对应的是操作的页数，考虑到并发情况，这应该是原子操作的，后面会提到
    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        // 调用父类 AbstractCache的构造方法，设置缓存的最大容量为 maxResource，初始化缓存相关的数据结构（HashMap等）
        // 意思就是最多缓存几个页面，因为一个文件所分割的页面，肯定不可能全部都加载到缓存中来。
        super(maxResource);
        // 检查内存限制，MEN_MIN_LIM是最小内存页数限制（通常为10）
        // 如果分配的内存太小，那么最多缓存的页面数也就太小，如果小于设定的最小页面数大小，直接 panic 终止程序，因为保证至少有足够的内存页用于基本操作
        //不然缓存也没有意义，会导致频繁的换入换出操作。
        if (maxResource < MEN_MIN_LIM) {
            Panic.panic(Error.MEM_TOO_SMALL_EXCEPTION);
        }
        //获取数据库文件的当前长度，文件操作可能抛出 IOException，捕获后 panic，这是为了后续计算现有页面数量
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 初始化文件相关成员
        this.file = file;
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
        //pageNumbers是通过db文件大小除以单页大小从而确定出来的
        // 计算当前页面数量：文件大小 / 页面大小。例如：8KB文件 / 4KB页大小 = 2页
        //pageNumbers是通过db文件大小除以单页大小从而确定出来的
        //这个字段是说，这个db文件可以分割为多少个页面
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }


    /**
     * 创建一个新的Page结构数据
     * 这里唯一要注意的是要根据当前Page的最大页数创建新页，同时在操作页码的时候要使用原子操作
     *
     * @param initData 新页面的初始化数据（字节数组）
     * @return 返回新分配的页面编号（pgno）
     */
    @Override
    public int newPage(byte[] initData) {
        //原子性地增加页面计数器，获取新的唯一页面编号（pgno）
        int pgno = pageNumbers.incrementAndGet();
        //使用给定的初始化数据创建新页面，第三个参数为null表示没有关联的PageCache（因为页面还未缓存）
        Page pg = new PageImpl(pgno, initData, null);
        // 将新页面立即写入磁盘文件中，保证新分配的页面不会丢失
        flush(pg);
        //返回新页面的编号供后续使用
        return pgno;
    }

    /**
     * 将Page写入到磁盘flush
     * 将Page数据写入到磁盘文件中，流程很易懂，就是获取页号，根据页号计算偏移，写入数据
     * 这里就是将对应的Page写入到磁盘文件中，从而保持磁盘中对应的Page是最新的数据
     * @param pg
     */
    private void flush(Page pg) {
        // 准备刷写
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);
        lock.lock();
        try {
            // 写入数据
            ByteBuffer buffer = ByteBuffer.wrap(pg.getData());
            fileChannel.position(offset);
            fileChannel.write(buffer);
            // 刷新磁盘
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 参数是pageNo即页号，获取的数据是这一页对应的在文件中的字节偏移量：
     * 由于页号从1开始，但在实际存储中是从0开始的，因此要先减1，再乘以页面大小（8KB）
     *
     * @param pgno 页面编号（从1开始）
     * @return 返回该页面在数据库文件中的字节偏移量（long类型）
     */
    private static long pageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    /**
     * 调用父类的缓存获取方法
     *
     * @param pgno 要获取的页面编号
     * @return 返回请求的Page对象
     * @throws Exception
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 页面释放方法
     * 这里调用的其实是抽象缓存中的Release，注意这里使用了模板方法设计模式，
     * 底层反过来在调用ReleaseForCache，从而实现ReleaseForCache的多样实现，后面还会有很多类似的实现
     *
     * @param page 要释放的页面对象
     */
    @Override
    public void release(Page page) {
        //从Page对象提取页面编号(pgno)
        //转换为long类型以匹配父类方法签名
        //调用AbstractCache.release(long)
        //最终会触发releaseForCache(Page)方法
        super.release((long) page.getPageNumber());
    }

    /**
     * 截断页面
     * 还是注意，页码数要使用原子操作
     * 截断文件，保留指定页数
     *
     * @param maxPgno 保留的最大页面编号
     */
    @Override
    public void truncateByPgno(int maxPgno) {
        //计算(maxPgno + 1)个页面对应的文件偏移量
        long size = pageOffset(maxPgno + 1);
        try {
            // 调整文件大小
            // 截断后部分数据将不可访问
            //意思就是只要最大页号及之前的数据了，之后的数据不要了，对于的操作就是重写计算文件长度，并赋值给file
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //原子性地更新页面计数器
        pageNumbers.set(maxPgno);
    }

    /**
     * 获取缓存的页数GetPageNumber
     * 这里要注意，为了原子操作，需要使用atomic包下的函数r
     *
     * @return 返回当前数据库文件中的页面总数（int类型）
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 页面刷写方法
     * 这里就是将对应的Page写入到磁盘文件中，从而保持磁盘中对应的Page是最新的数据
     *
     * @param pg 要刷写的页面对象
     */
    @Override
    public void flushPage(Page pg) {
        //简单委托给内部flush()方法，提供public接口给外部调用
        flush(pg);
    }

    /**
     * 实际页面加载(getForCache)
     * 负责缓存的底层数据加载
     * <p>
     * 实现抽象缓存——GetForCache
     * 上面提到了页面缓存是要实现抽象缓存接口的，
     * 因此要实现GetForCache和ReleaseForCache，实现具体的获取缓存数据和驱逐缓存数据的操作
     * <p>
     * 根据页号读取一个页面到缓存内存中。数据源其实就是文件，因此这里就是从文件中读取一个页面大小的数据并且包裹成Page对象即可
     *
     * @param key 页面编号（转换为long类型）
     * @return 加载后的Page对象
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        // 计算页面在文件中的偏移量，将long类型的key转回int类型的页面编号
        int pgno = (int) key;
        //使用静态方法计算页面在文件中的位置
        long offset = pageOffset(pgno);
        // 准备缓冲区，分配与页面大小匹配的缓冲区，使用NIO的ByteBuffer提高I/O效率
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        // 加锁保证线程安全，加锁保证文件操作的原子性
        lock.lock();
        try {
            // 定位并读取文件，定位到指定偏移量
            fileChannel.position(offset);
            //读取页面数据到缓冲区
            fileChannel.read(buffer);
        } catch (IOException e) {
            //异常时直接panic终止
            Panic.panic(e);
        } finally {
            //finally块确保锁释放
            lock.unlock();
        }
        // 创建Page对象并返回,关联当前PageCache实例
        return new PageImpl(pgno, buffer.array(), this);
    }

    /**
     * 页面释放处理 (releaseForCache)
     * 注意：这里只是释放缓存，Page对象可能仍然被其他部分引用
     * 要判断一下Page是不是脏页，如果是再写入磁盘
     *
     * @param pg 待释放的页面对象
     */
    @Override
    protected void releaseForCache(Page pg) {
        // 检查是否为脏页（被修改过）
        if (pg.isDirty()) {
            // 写回磁盘
            flush(pg);
            // 清除脏页标志
            pg.setDirty(false);
        }
    }
}
