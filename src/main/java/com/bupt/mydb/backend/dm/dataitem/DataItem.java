package com.bupt.mydb.backend.dm.dataitem;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.dm.DataManagerImpl;
import com.bupt.mydb.backend.dm.page.Page;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.common.Types;
import com.google.common.primitives.Bytes;

import javax.xml.crypto.Data;
import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/17 20:50
 * description:
 * DataItem 是一个数据抽象层，它提供了一种在上层模块和底层数据存储之间进行交互的接口
 * ，其作用和功能主要包括：
 * **数据存储和访问**：DataItem存储了数据的具体内容，
 * 以及一些相关的元数据信息，如数据的大小、有效标志等。上层模块可以通过 DataItem 对象获取到其中的数据内容，
 * 以进行读取、修改或删除等操作。
 * **数据修改和事务管理**：DataItem 提供了一些方法来支持数据的修改操作，并在修改操作前后执行一系列的流程，
 * 如保存原始数据、落日志等。这些流程保证了数据修改的原子性和一致性，同时支持事务管理，确保了数据的安全性。
 * **数据共享和内存管理**：DataItem 的数据内容通过 SubArray 对象返回给上层模块，这使得上层模块可以直接访问数据内容而无需进行拷贝。
 * 这种数据共享的方式提高了数据的访问效率，同时减少了内存的开销。
 * 4. **缓存管理**：DataItem 对象由底层的 DataManager 缓存管理，通过调用 release() 方法可以释放缓存中的 DataItem 对象，
 * 以便回收内存资源，提高系统的性能和效率。
 * <p>
 * 主要功能
 * 数据访问：提供对底层数据的访问能力
 * 事务支持：提供事务相关的操作（before/after）
 * 并发控制：提供读写锁机制
 * 数据管理：管理数据的有效性、释放等
 * 数据解析：提供静态方法用于数据项的解析和包装
 * <p>
 * DataItem是存储于db文件中的逻辑概念，数据库的数据以DataItem的形式存储在文件系统中
 * DataItem
 * 存储在db文件中的数据都是以DataItem的形式存储的，其中功能如下：
 * <p>
 * 数据的存取：DataItem存储了数据内容以及元信息。上层模块通过DataItem获取到其中的内容，进行读取、修改、删除等操作
 * 数据修改和事务管理：DataItem提供了一些api来操作数据；还有在修改数据的操作前后执行一些预操作和后操作，如保存原始数据、存储日志等。从这些角度保证了数据修改的原子性、一致性，还支持事务管理等
 * 数据共享和内存管理： DataItem存储的实际数据可以通过切片直接返回给上层模块，上层模块不需要复制数据，而是保存了一个切片的引用，对数据的访问和操作都是同步的
 * 缓存管理：DataItem由DataManager缓存管理，因此可以有较高的性能
 * 简单来说，**DataItem**就是对数据的再一次封装，隐藏了存储的细节；
 * 上层模块操作数据都通过**DataItem**相关的API来操作
 * <p>
 * <p>
 * DataItem定义
 * DataItem中的数据
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * <p>
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize 2字节，标识Data的长度
 * <p>
 * UID 结构如下:
 * [pageNumber] [空] [offset]
 * <p>
 * pageNumber 4字节，页号
 * 中间空下2字节
 * offset 2字节，偏移量
 * <p>
 * 这里的UID实际上就是由页号+页内偏移组成的，可以通过此唯一标识一个**DataItem**
 */
public interface DataItem {
    /**
     * 获取数据项的数据内容（返回SubArray）
     *
     * @return
     */
    SubArray data();

    /**
     * 开始修改前的准备（通常用于记录旧值）
     */
    void before();

    /**
     * 撤销before操作
     */
    void unBefore();

    /**
     * 修改完成后的操作（通常与事务ID关联）
     */
    void after(long xid);

    /**
     * 释放数据项资源
     */
    void release();

    /**
     * 获取排他锁
     */
    void lock();

    /**
     * 释放排他锁
     */
    void unLock();

    /**
     * 获取共享锁（读锁）
     */
    void rLock();

    /**
     * 释放共享锁
     */
    void rUnLock();

    /**
     * 获取数据项所在的页面
     *
     * @return
     */
    Page page();

    /**
     * 获取数据项的唯一标识符
     *
     * @return
     */
    long getUid();

    /**
     * 获取修改前的原始数据（用于回滚）
     *
     * @return
     */
    byte[] getOldRaw();

    /**
     * 获取原始数据（返回SubArray）
     *
     * @return
     */
    SubArray getRaw();

    /**
     * 包装原始数据，添加有效位和长度信息
     *
     * @param raw
     * @return
     */
    // WrapDataItemRaw 包装数据项，将标志位和长度加到原始数据的前面，从而返回符合DataItem格式的数据
    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 这个方法的作用是从数据库页面的指定偏移量（offset）处解析出一个数据项（DataItem）。
     * 它是数据库存储引擎中用于从磁盘页面上读取数据记录的关键方法。
     *
     * @param page   包含数据的数据库页面对象
     * @param offset 数据项在页面中的起始偏移量
     * @param dm     数据管理器，用于管理数据项的生命周期
     * @return
     */
    // ParseDataItem 解析page页中的数据从而得到数据项
    static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        // 获取该页的字节数据
        byte[] raw = page.getData();
        //从数据项的固定位置（OF_SIZE到OF_DATA）读取2字节的数据长度字段
        //这个size表示数据项中实际存储的数据长度（不包括头部信息）
        // 从offset开始解析数据，要解析出一个dataItem
        // 先从offset开始解析dataItem的size
        //数据长度
        short size = Parser.parseShort(
                Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE,
                        offset + DataItemImpl.OF_DATA));
        //OF_DATA是数据项头部信息的固定大小（通常包含有效位、长度等信息）
        //总长度 = 数据长度 + 头部长度
        // size能够得出dataItem的长度，现在需要将dataItem在页中的数据所处的位置解析出来
        // length对应的是dataItem的长度，加上offset就是dataItem的结束位置
        short length = (short) (size + DataItemImpl.OF_DATA);
        //使用页面号和偏移量组合生成一个唯一ID，用于标识这个数据项
        // 生成UID
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        //创建一个新的DataItemImpl实例
        //参数1：数据内容（使用SubArray包装原始数据的一部分），还是一整页的数据，只不过加上了起始位置和结束位置，指明一个DataItem的范围
        //参数2：用于存储旧数据的缓冲区（用于事务回滚）
        //参数3：所属页面
        //参数4：唯一ID
        //参数5：数据管理器
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dm);
    }

    /**
     * 这个方法的作用是将一个数据项（DataItem）标记为无效，
     * 通常用于数据库系统中的数据删除或事务回滚操作。
     *
     * @param raw
     */
    // SetDataItemRawInValid 设置数据项为失效
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }

}
