package com.bupt.mydb.backend.tbm;


import com.bupt.mydb.backend.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import com.bupt.mydb.common.Error;

/**
 * 之前的章节提到了数据库的数据都存储在db文件中，
 * 日志文件存储在log文件，事务存储在xid文件，
 * 但是数据都是由uid中的页号+页偏移定位的。
 * 那么第一个uid哪里来？这就是Booter的作用，他存储了第一个数据表的uid，从而开始了后面的各种操作
 */
// 记录第一个表的uid
public class Booter {
    // BooterSuffix 数据库启动信息文件的后缀
    public static final String BOOTER_SUFFIX = ".bt";
    // BooterTmpSuffix 数据库启动信息文件的临时后缀
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";
    // 数据库启动信息文件的路径
    String path;
    // 数据库启动信息文件
    File file;

    // CreateBooter 创建一个新的Booter对象
    public static Booter create(String path) {
        // 删除可能存在的临时文件
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FILE_EXISTS_EXCEPTION);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 检查文件是否可读写，如果不可读写，则抛出异常
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        // 返回一个新的数据库启动信息对象
        return new Booter(path, f);
    }

    // OpenBooter 打开一个已经存在的Booter对象
    public static Booter open(String path) {
        // 删除可能存在的临时文件
        removeBadTmp(path);
        // 创建一个新的文件对象，文件名是路径加上启动信息文件的后缀
        File f = new File(path + BOOTER_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FILE_NOT_EXISTS_EXCEPTION);
        }
        // 检查文件是否可读写，如果不可读写，则抛出异常
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        // 返回一个新的数据库启动信息对象
        return new Booter(path, f);
    }

    // RemoveBatTmpBooter 删除可能存在的临时文件
    private static void removeBadTmp(String path) {
        // 删除路径加上临时文件后缀的文件
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    // Load 加载文件启动信息文件,其实就是把对应文件读进来
    public byte[] load() {
        byte[] buf = null;
        try {
            // 创建一个大小为文件大小的字节数组
            // 读取文件的所有字节到data中
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    // Update 更新启动信息文件的内容
    public void update(byte[] data) {
        // 创建一个新的临时文件
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 检查文件是否可读写，如果不可读写，则抛出异常
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        // 将data写入临时文件
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 将临时文件移动到启动信息文件的位置，替换原来的文件
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 重新打开目标文件
        file = new File(path + BOOTER_SUFFIX);
        // 检查新的启动信息文件是否可读写，如果不可读写，则抛出异常
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
    }

}
