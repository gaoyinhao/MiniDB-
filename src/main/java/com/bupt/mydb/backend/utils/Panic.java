package com.bupt.mydb.backend.utils;

/**
 * @author gao98
 * date 2025/9/4 21:40
 * description:
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
