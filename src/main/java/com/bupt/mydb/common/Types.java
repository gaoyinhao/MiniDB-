package com.bupt.mydb.common;

/**
 * @author gao98
 * date 2025/9/19 11:14
 * description:
 */
public class Types {

    /**
     * 这个方法的作用是将 页面号（pgno）和页面内偏移量（offset）
     * 组合成一个唯一的 UID（唯一标识符），用于在数据库中精确定位某个数据项的位置。
     *
     * @param pgno   页面号（Page Number）
     * @param offset 数据项在页面内的偏移量（字节偏移）
     * @return
     */
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long) pgno;
        long u1 = (long) offset;
        return u0 << 32 | u1;
    }
}
