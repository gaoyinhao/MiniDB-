package com.bupt.mydb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/4 22:23
 * description:
 */
public class Parser {

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf) {
        // 将 buf 的前 2 个字节包装成 ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        // 从 ByteBuffer 读取一个 short（2字节）
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        return ByteBuffer.wrap(buf, 0, 4).getInt();
    }

    /**
     * 将数组前八位转换成长整数
     *
     * @param buf 需要转换的字节数组
     * @return 转换后的数据
     */
    public static long parseLong(byte[] buf) {
        // 一个包含8个字节的字节数组
        //因为long 在Java中占用8个字节，每个字节占用8位，一下数组可以转换成一个long数字
        // 00000000 00000000 00000000 00000000 00000000 00000000 00001010 00000001
        // 1010 00000001 --> 2561
        // 使用ByteBuffer.wrap方法将字节数组包装为一个ByteBuffer对象
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        // 使用getLong方法从ByteBuffer中读取一个长整型数
        return buffer.getLong();
    }

//    public static void main(String[] args) {
//        testBufferGetLong();
//    }

    public static void testBufferGetLong() {

        // 创建一个包含8个字节的字节数组
        //因为long 在Java中占用8个字节，每个字节占用8位，一下数组可以转换成一个long数字
        // 00000000 00000000 00000000 00000000 00000000 00000000 00001010 00000001
        // 1010 00000001 --> 2561
        byte[] byteArray = new byte[]{0, 0, 0, 0, 0, 0, 10, 1};
        // 使用ByteBuffer.wrap方法将字节数组包装为一个ByteBuffer对象
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);

        // 使用getLong方法从ByteBuffer中读取一个长整型数
        long longValue = buffer.getLong();

        // 输出读取的长整型数
        System.out.println("The long value is: " + longValue);
    }

    /**
     * 将长整型值写入到字节缓冲区，将其转成为8字节的二进制形式，然后将这个8个字节写入到字节缓冲区
     *
     * @param value
     * @return
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParseStringRes(str, length);
    }

    // String2Bytes 将字符串转换为字节数组，格式为：[StringLength][StringData]，前者4字节
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }

}
