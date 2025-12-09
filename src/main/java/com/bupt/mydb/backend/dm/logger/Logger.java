package com.bupt.mydb.backend.dm.logger;

import com.bupt.mydb.backend.dm.pageCache.PageCacheImpl;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.common.Error;
import com.google.common.collect.RangeMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author gao98
 * date 2025/9/17 22:00
 * description:
 * MYDB提供了崩溃后的数据恢复功能，DM层在每次对底层数据进行操作时，
 * 都会记录一条日志到磁盘上。在数据库崩溃之后，
 * 再次重启时，可以根据日志的内容，恢复数据文件，保证其一致性；
 * <p>
 * Logger是数据库事务日志的核心接口，定义了日志系统的基本操作。
 * 它的两个静态工厂方法 create()和 open()
 * 分别用于创建新日志和打开已有日志。
 * <p>
 * 日志文件在通常情况下不会发挥作用，但是在数据库崩溃时可以使用其来进行数据恢复。
 * 数据管理器（即DM）在每一次操作底层数据时，都会生成一条日志。如果数据库崩溃了，
 * 下次重启会根据日志内容进行恢复
 */
public interface Logger {
    // 记录日志数据,事务提交时记录操作日志
    void log(byte[] data);

    // 截断日志到指定位置,	日志回收或崩溃恢复后清理
    void truncate(long x) throws Exception;

    // 读取下一条日志,崩溃恢复时重放事务
    byte[] next();

    // 重置日志读取位置,重新读取日志
    void rewind();

    // 关闭日志文件,数据库正常关闭时
    void close();

    // CreateLogger 创建一个新的日志管理器
    static Logger create(String path) {
        // 创建日志文件，不能存在同名文件
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(Error.FILE_EXISTS_EXCEPTION);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        // 创建日志文件，写入一开始的校验和（文件起始部分四字节的0）
        ByteBuffer byteBuffer = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fileChannel.position(0);
            fileChannel.write(byteBuffer);
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(randomAccessFile, fileChannel, 0);
    }

    // OpenLogger 打开一个已经存在的日志文件
    static Logger open(String path) {
        // 日志文件必须存在
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!file.exists()) {
            Panic.panic(Error.FILE_NOT_EXISTS_EXCEPTION);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        LoggerImpl logger = new LoggerImpl(randomAccessFile, fileChannel);
        //日志文件初始化操作
        logger.init();
        return logger;
    }
}
