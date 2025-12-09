package com.bupt.mydb.backend.dm;

import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.dm.logger.Logger;
import com.bupt.mydb.backend.dm.page.PageOne;
import com.bupt.mydb.backend.dm.pageCache.PageCache;
import com.bupt.mydb.backend.tm.TransactionManager;

/**
 * @author gao98
 * date 2025/9/17 21:39
 * description:
 * 这是数据库系统的数据管理器接口，负责管理数据项的存储、读取和事务处理。
 * 它提供了数据库的核心数据管理功能，
 * 包括创建新数据库、打开已有数据库、读取数据和插入数据等操作。
 * <p>
 * DM层在操作数据时，使用的是DataItem的形式，而DataManager则是用来对上层其他模块提供数据操作的API的；
 * 另外，DataManager还提供了对DataItem的缓存机制
 */
public interface DataManager {
    /**
     * 通过UID读取一个数据项
     *
     * @param uid 数据项的唯一标识符（由页面号和偏移量组成）
     * @return DataItem对象
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    DataItem read(long uid) throws Exception;

    /**
     * 插入新数据
     *
     * @param xid  事务ID
     * @param data 要插入的字节数据
     * @return 新数据项的UID
     * @throws Exception 可能抛出I/O异常或其他数据库异常
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 关闭数据管理器，释放资源
     */
    void close();


    /**
     * 如下两种不同方式需要注意：
     *
     * 从空文件创建需要对第一页进行初始化
     * 从已有文件创建（即打开），需要对第一页进行校验从而判断是否需要执行恢复流程，并重新对第一页生成随机字节
     */
    /**
     * 创建一个新的数据库
     *
     * @param path               数据库文件路径
     * @param mem                内存限制（字节）
     * @param transactionManager 事务管理器
     * @return
     */
    // CreateDataManager 创建数据管理器，从这里才会创建一个页缓存器，日志记录器，数据管理对象。而事务管理器是上一个模块的内容，因此之前就会创建好，在这里传入
    static DataManager create(String path, long mem, TransactionManager transactionManager) {
        // 创建页缓存(PageCache)，在指定路径创建一个新的页缓存文件，mem参数限制内存使用量
        // 创建一个PageCache实例，path是文件路径，mem是内存大小
        PageCache pageCache = PageCache.create(path, mem);
        //创建日志(Logger)，初始化事务日志系统
        // 创建一个Logger实例，path是文件路径
        Logger logger = Logger.create(path);
        //创建数据管理器，将页缓存、日志和事务管理器绑定到数据管理器
        // 创建一个DataManager实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        // 初始化PageOne，为什么会额外初始化PageOne呢，因为数据库的第一页(PageOne)用于校验数据库文件(db文件)是否出现错误，因此需要初始化
        //初始化的意思就是新建一个页面，设置页号位1，然后将整页全部设置为0，然后存到磁盘上
        dataManager.initPageOne();
        //返回初始化好的DataManager实例
        // 返回创建的DataManagerImpl实例
        return dataManager;
    }

    /**
     * 打开一个已存在的数据库
     *
     * @param path               数据库文件路径
     * @param mem                内存限制（字节）
     * @param transactionManager 事务管理器
     * @return
     */
    // OpenDataManager 打开一个数据管理器
    static DataManager open(String path, long mem, TransactionManager transactionManager) {
        // 打开一个PageCache实例，path是文件路径，mem是限定所使用的内存大小
        PageCache pageCache = PageCache.open(path, mem);
        // 打开一个Logger实例，path是文件路径
        Logger logger = Logger.open(path);
        // 创建一个DataManager 实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        //检查第一页有效性，如果上次数据库异常关闭，执行恢复流程
        // 加载并检查PageOne，如果检查失败，则进行恢复操作，
        // 所谓的恢复流程就是逐条读取日志，找到能找到的最大页号，然后让db文件按这个最大页号截断，
        // 意思是认为最大页号之后的数据都是异常的数据，直接全都不要了
        // 然后按照日志文件中的每一条日志对应的事务的状态，进行redo重做操作或undo撤销操作
        if (!dataManager.loadCheckPageOne()) {
            Recover.recover(transactionManager, logger, pageCache);
        }
        // 填充PageIndex，遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中
        // 为什么从第二页开始呢，因为Pgeone是有特殊用途的，主要是为了校验数据库上次关闭是否出现了错误，
        // 所以第一页不能用于存储数据
        dataManager.fillPageIndex();
        //标记数据库为已打开，更新PageOne的状态标志，对应于关闭的时候，也要同步设置数据库为关闭，
        // 这样的意思是指数据库正常打开和关闭了
        // 设置PageOne为打开状态
        //从已有文件创建（即打开），需要对第一页进行校验从而判断是否需要执行恢复流程，
        // 并重新对第一页生成随机字节
        PageOne.setVcOpen(dataManager.pageOne);
        // 将修改后的PageOne立即写入到磁盘中，确保PageOne的数据被持久化
        dataManager.pageCache.flushPage(dataManager.pageOne);
        //返回数据管理器
        // 返回创建的DataManager实例
        return dataManager;
    }


}
