package com.bupt.mydb.backend.dm.page;

import com.bupt.mydb.backend.dm.pageCache.PageCache;
import com.bupt.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/19 22:14
 * description:
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 * <p>
 * PageX是数据库系统中页面空间管理的核心工具类，负责处理数据页面的初始化、空间分配和数据操作。
 * +--------+----------------------------+
 * | FSO(2) |         数据区            |
 * +--------+----------------------------+
 * 0        2                           PAGE_SIZE
 * FSO(Free Space Offset)：2字节，记录空闲数据区开始位置
 * 数据区：PAGE_SIZE - 2字节，存储实际数据
 * 内存布局示例：
 * [0x00,0x02, 0xAA,0xBB,0xCC, ...]
 * 表示：
 * - FSO = 2 (0x0002)
 * - 空闲数据区从偏移2开始
 * <p>
 * 其他页：
 * 之前提到了一个Page的大小是固定的，对于普通页也是一样的，但是在一个Page的内部也需要区分数据区和空闲区。为此有以下定义：
 * 在一个页内的数据是按照顺序存储的，而非随机存储的
 * 普通页以2个字节起始，表示该页内的空闲区的起始偏移量
 * 从空闲区起始偏移量开始直到该页结束为空闲区，可以用来存储新数据
 *
 *
 * 这个是一个普通页的相关操作，唯一需要注意的是时刻记住页面一开始有两个字节是用来表示空闲位置的偏移量的
 */
public class PageX {
    // 空闲空间偏移量位置，标记页面中空闲空间偏移量(FSO)的存储位置
    //  表示页面空闲位置的起始偏移量,大白话就是一个页面，前2个字节的含义是说目前该页面空闲空间的起始位置是多少
    private static final short OF_FREE = 0;
    // 实际数据起始位置，标记页面中实际数据区的起始位置
    // 数据的起始偏移量
    private static final short OF_DATA = 2;
    // 最大空闲空间，页面初始化时的可用空间最大值，单位字节也就是8192-2=8190 字节
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * initRaw()是数据库页面初始化的重要方法，负责创建并初始化一个全新的数据页。
     *
     * @return 返回一个符合数据库规范的初始化页面字节数组
     */
    // 初始化一个普通页面
    public static byte[] initRaw() {
        // 1. 创建新数组
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 2. 设置初始FSO
        setFSO(raw, OF_DATA);
        // 3. 返回初始化后的页面
        return raw;
    }

    /**
     * 设置空闲空间偏移量
     * 将指定的偏移量写入页面头部
     * 更新空闲空间管理信息
     *
     * @param raw    目标页面字节数组
     * @param ofData 新的空闲空间偏移量（通常指向数据区末尾）
     */
    private static void setFSO(byte[] raw, short ofData) {
        //将short转为2字节数组，将转换后的字节写入页面头部
        //在前两字节设置页面的空闲位置的起始偏移量
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取空闲空间偏移量
     * 提供Page对象和byte[]两种参数形式
     * 获取pg的FSO
     *
     * @param pg
     * @return
     */
    public static short getFSO(Page pg) {
        //获得页面当前的空闲位置的起始偏移量
        return getFSO(pg.getData());
    }

    /**
     * 从页面头部解析出当前空闲空间偏移量
     *
     * @param raw
     * @return
     */
    private static short getFSO(byte[] raw) {
        //  根据原始数据的前两个字节获得空闲位置的起始偏移量
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        // 1. 标记脏页
        pg.setDirty(true);
        // 2. 获取当前插入位置，获取页面的空闲位置偏移量
        short offset = getFSO(pg.getData());
        // 3. 拷贝数据，将raw中从0开始到最后，copy到pg的data中从offset开始，长为raw.length中
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 4. 更新页面中前两个字节的值，因为插入了新数据，所以空闲空间的的起始位置也应该更新
        //  更新新的空闲位置
        setFSO(pg.getData(), (short) (offset + raw.length));
        // 5. 返回插入位置
        return offset;
    }

    /**
     * 计算并返回页面当前可用空间大小
     * 空闲空间 = 总页大小 - 当前FSO的值
     * 获取页面的空闲空间大小
     *
     * @param pg 目标页面对象
     * @return
     */
    public static int getFreeSpace(Page pg) {
        //获得页面的剩余空间字节数
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     * 恢复插入数据
     * 将raw插入pg中的offset位置，并将pg的offset设置为新的offset
     *
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        // 1. 标记脏页
        pg.setDirty(true);
        // 2. 写入数据
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 3. 获取当前FSO
        short rawFSO = getFSO(pg.getData());
        // 4. 检查边界，如果rawFSO的值小于offset+raw.Length,则更新当前FSO
        if (rawFSO < offset + raw.length) {
            // 5. 扩展FSO
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }


    /**
     * 在指定位置覆盖写入数据，不涉及到新增数据，应该不更新前两个字节的值
     *
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
