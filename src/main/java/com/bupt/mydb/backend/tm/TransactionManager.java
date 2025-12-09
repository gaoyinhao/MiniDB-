package com.bupt.mydb.backend.tm;

/**
 * ## XID文件
 * 1. **XID 的定义和规则：**
 * - 每个事务都有一个唯一的事务标识符 XID，从 1 开始递增，
 * 并且 XID 0 被特殊定义为超级事务（Super Transaction）。
 * - XID 0 用于表示在没有申请事务的情况下进行的操作，其状态永远是 committed。
 * 2. **事务的状态：**
 * - 每个事务可以处于三种状态之一：active（正在进行，尚未结束）、committed（已提交）和aborted（已撤销或回滚）。
 * 3. **XID 文件的结构和管理：**
 * - TransactionManager 负责维护一个 XID 格式的文件，用于记录各个事务的状态。
 * - XID文件中为每个事务分配了一个字节的空间，用来保存其状态。
 * - XID文件的头部保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数。
 * - 因此，事务 XID 在文件中的状态存储在 (XID-1)+8 字节的位置处，其中 XID-1
 * 是因为 XID 0（超级事务）的状态不需要记录。
 */

import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.common.Error;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author gao98
 * date 2025/9/4 21:43
 * description: 提供了一些接口供其他模块调用，用来创建事务和查询事务的状态；
 */
public interface TransactionManager {
    //开启事务
    long begin();

    //提交事务
    void commit(long xid);

    //撤销或者回滚事务
    void abort(long xid);

    //查询一个事务的状态是否正在运行
    boolean isActive(long xid);

    //查询一个事务的状态是否已经提交
    boolean isCommitted(long xid);

    //查询一个事务的状态是否撤销或者回滚
    boolean isAborted(long xid);

    //关闭事务
    void close();

    static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FILE_EXISTS_EXCEPTION);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (IOException e) {
            Panic.panic(e);
        }
        //写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            buf.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

    static TransactionManagerImpl open(String path) {
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!file.exists()) {
                Panic.panic(Error.FILE_NOT_EXISTS_EXCEPTION);
            }
            if (!file.canRead() || !file.canWrite()) {
                Panic.panic(Error.FILE_CAN_NOT_RW_EXCEPTION);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (Exception e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

}
