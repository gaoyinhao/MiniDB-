package com.bupt.mydb.backend.common;

/**
 * @author gao98
 * date 2025/9/17 21:27
 * description:
 * ## 原文
 * 这里得提一个 Java 很蛋疼的地方。
 * Java 中，将数组看作一个对象，在内存中，也是以对象的形式存储的。而 c、cpp 和 go 之类的语言，数组是用指针来实现的。这就是为什么有一种说法：
 * 只有 Java 有真正的数组
 * 但这对这个项目似乎不是一个好消息。譬如 golang，可以执行下面语句：
 * <p>
 * ```go
 * var array1 [10]int64
 * array2 := array1[5:]
 * ```
 * Copy
 * 这种情况下，array2 和 array1 的第五个元素到最后一个元素，是共用同一片内存的，即使这两个数组的长度不同。
 * 这在 Java 中是无法实现的（什么是高级语言啊~）。
 * 在 Java 中，当你执行类似 subArray 的操作时，只会在底层进行一个复制，无法同一片内存。
 * 于是，我写了一个 SubArray 类，来（松散地）规定这个数组的可使用范围：
 * ```java
 */
//其实就是raw放了一整个页面的数据，而start和end是指明该页指定位置范围的DataItem数据罢了，一个DataItem的格式为：[valid](1字节)[size](2字节)[data]
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }

}
