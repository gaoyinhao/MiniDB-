package com.bupt.mydb.backend.tm;

import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/4 21:52
 * description:
 */
public class TransactionManagerImpl implements TransactionManager {
    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务的占用长度为一字节
    private static final int XID_FIELD_SIZE = 1;
    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    //超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    //XID文件后缀
    static final String XID_SUFFIX = ".xid";
    //XID文件
    private RandomAccessFile file;
    //XID文件通道
    private FileChannel fileChannel;
    //事务总数
    private long xidCounter;
    //控制并发锁
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * checkXIDCounter
     * 在构造函数创建了一个 TransactionManager 之后，首先要对 XID 文件进行校验，
     * 以保证这是一个合法的 XID 文件。校验的方式也很简单，通过文件头的 8 字节数字反推文件的理论长度，
     * 与文件的实际长度做对比。如果不同则认为 XID 文件不合法。对于校验没有通过的，会直接通过 panic 方法，
     * 强制停机。在一些基础模块中出现错误都会如此处理，无法恢复的错误只能直接停机。
     * <p>
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     *
     * @return
     */
    private void checkXIDCounter() {
        //初始化文件长度为0
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BADXID_FILE_EXCEPTION);
        }
        //如果文件长度小于XID头部长度,抛出BadXIDFileException错误
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BADXID_FILE_EXCEPTION);
        }
        //分配一个长度为XID头部长度的ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            //将文件通道的位置设置为0
            fileChannel.position(0);
            //从文件通道读取数据到ByteBuffer
            fileChannel.read(buffer);
        } catch (IOException e) {
            //如果出现异常,抛出错误
            Panic.panic(Error.BADXID_FILE_EXCEPTION);
        }
        //将ByteBuffer的内容解析为长整型,作为xidCounter
        this.xidCounter = Parser.parseLong(buffer.array());
        //计算xidCounter+1对应的XID位置
        long end = getXidPosition(this.xidCounter + 1);
        //如果计算出的XID位置与文件长度不符，抛出错误
        if (end != fileLen) {
            Panic.panic(Error.BADXID_FILE_EXCEPTION);
        }
    }

    //根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    /**
     * 更新xid事务的状态为status
     * 更新事务`ID`状态,`**commit()**`和** abort() **方法就可以直接借助 `**updateXID()**` 方法实现。
     *
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        // 获取事务xid在xid文件中对应的位置
        long offset = getXidPosition(xid);
        // 创建一个长度为XID_FIELD_SIZE的字节数组
        byte[] temp = new byte[XID_FIELD_SIZE];
        // 将事务状态设置为status
        temp[0] = status;
        // 使用字节数组创建一个ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(temp);
        try {
            // 将文件通道的位置设置为offset
            fileChannel.position(offset);
            // 将ByteBuffer中的数据写入到文件通道
            fileChannel.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制将文件通道中的所有未写入的数据写入到磁盘
            // 调用 force() 会强制将内存缓冲区（缓存）中的数据写入磁盘，防止程序崩溃或系统断电导致数据丢失。
            //参数 false: 表示只同步文件内容,但不强制更新文件的元数据(metadata)（如修改时间、权限等）,
            // 如果传 true，则同时强制更新元数据。
            fileChannel.force(false);
        } catch (IOException e) {
            // 如果出现异常，调用Panic.panic方法处理
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        // 事务总数加一
        xidCounter++;
        // 将新的事务总数转换为字节数组，并用ByteBuffer包装
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            // 将文件通道的位置设置为0，即文件的开始位置
            fileChannel.position(0);
            // 将ByteBuffer中的数据写入到文件通道，即更新了XID文件的头部信息
            fileChannel.write(buffer);
        } catch (IOException e) {
            // 如果出现异常，调用Panic.panic方法处理
            Panic.panic(e);
        }
        try {
            // 强制将文件通道中的所有未写入的数据写入到磁盘
            fileChannel.force(false);
        } catch (IOException e) {
            // 如果出现异常，调用Panic.panic方法处理
            Panic.panic(e);
        }

    }

    //isActive()、isCommitted() **`和 `**isAborted()**` 都是检查一个 **xid** 的状态
    //定义一个方法，接收一个事务ID（xid）和一个状态（status）作为参数
    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        // 计算事务ID在XID文件中的位置
        long offset = getXidPosition(xid);
        // 创建一个新的字节缓冲区（ByteBuffer），长度为XID_FIELD_SIZE
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            // 将文件通道的位置设置为offset
            fileChannel.position(offset);
            // 从文件通道读取数据到字节缓冲区
            fileChannel.read(buffer);
        } catch (IOException e) {
            // 如果出现异常，调用Panic.panic方法处理
            Panic.panic(e);
        }
        // 检查字节缓冲区的第一个字节是否等于给定的状态
        // 如果等于，返回true，否则返回false
        return buffer.array()[0] == status;
    }

    /**
     * `**begin()**` 方法会开始一个事务，更具体的
     * ，首先设置 `xidCounter+1` 事务的状态为 `active`，随后 `xidCounter` 自增，并更新文件头。
     *
     * @return
     */
    // 开始一个事务，并返回XID
    @Override
    public long begin() {
        //锁定计数器，防止并发问题
        counterLock.lock();
        try {
            // xidCounter是当前事务的计数器，每开始一个新的事务，就将其加1
            long xid = xidCounter + 1;
            // 调用updateXID方法，将新的事务ID和事务状态（这里是活动状态）写入到XID文件中
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 调用incrXIDCounter方法，将事务计数器加1，并更新XID文件的头部信息
            incrXIDCounter();
            // 返回新的事务ID
            return xid;
        } finally {
            counterLock.unlock();
        }
    }



    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return true;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
