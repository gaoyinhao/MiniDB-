package com.bupt.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author gao98
 * date 2025/9/20 22:51
 * description:
 */
public class RandomUtil {
    public static byte[] randomBytes(int length) {
        // 创建安全随机数生成器
        Random r = new SecureRandom();
        // 分配目标数组
        byte[] buf = new byte[length];
        // 填充随机字节
        r.nextBytes(buf);
        return buf;
    }
}
