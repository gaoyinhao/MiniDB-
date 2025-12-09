package com.bupt.mydb.backend.dm.logger;

import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.common.Error;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/19 22:13
 * description:
 * <p>
 * 日志文件
 * 日志的二进制文件，按照如下的格式进行排布：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * 其中 `**XChecksum**` 是一个四字节的整数，是对后续所有日志计算的校验和。
 * `**Log1 ~ LogN**` 是常规的日志数据，
 * `**BadTail**` 是在数据库崩溃时，没有来得及写完的日志数据，这个 `**BadTail**` 不一定存在。
 * 最后是可能存在的损坏的日志条目，在重启后通过校验要将其截断
 * 每条日志的格式如下：
 * <p>
 * 每条正确日志(Log)的格式为：
 * * [Size] [Checksum] [Data]
 * Size 4字节int 表示后面Data的字节数
 * Checksum 4字节in t是对本条日志条目的校验和，注意和全局的校验和**XCheckSum****进行区分 **
 * DATA 具体的日志数据

 * 文件整体结构
 * | 全局校验和(4)   | 日志记录1      | 日志记录2      |
 * 文件开头4字节存储全局校验和
 * 后续为连续的日志记录
 */
public class LoggerImpl implements Logger {
    private static final int SEED = 13331;
    // 每个日志条目的数据结构
    //| 长度(4) | 校验和(4) | 数据(N)   |
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    // LogSuffix 日志文件后缀
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile randomAccessFile;

    private FileChannel fileChannel;

    private Lock lock;
    // 当前日志指针的位置,	当前读取位置
    private long position;
    // 初始化时记录，log操作不更新,日志文件大小
    // 初始化时记录一下，当进行log操作时不更新此值
    private long fileSize;
    // xCheckSum 全局校验和，文件级别的校验和
    private int xChecksum;


    // NewLogger 创建一个新的日志管理器
    public LoggerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel, int xChecksum) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        //设置文件开头的xchecksum的值
        this.xChecksum = xChecksum;
        this.lock = new ReentrantLock();
    }

    /**
     * 在打开已有日志文件时进行完整性检查和状态恢复。
     * init 对日志文件进行初始化，主要是获取文件大小并进行文件大小与校验和的校验
     */
    public void init() {
        long size = 0;
        try {
            //通过RandomAccessFile.length()获取文件大小
            // 获取文件大小
            size = randomAccessFile.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 小于4字节说明连前面的校验和都没有
        //文件至少需要4字节存储全局校验和,不满足则判定为损坏的日志文件
        //若文件大小小于4，证明日志文件创建出现问题
        if (size < 4) {
            Panic.panic(Error.BAD_LOG_FILE_EXCEPTION);
        }
        // 分配4字节缓冲区
        // 获取前面4字节的校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            //定位到文件开头读取校验和
            fileChannel.position(0);
            fileChannel.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //将前四个字节的数据解析为int类型
        int xChecksum = Parser.parseInt(raw.array());
        //保存文件大小和文件全局校验和
        this.fileSize = size;
        this.xChecksum = xChecksum;
        //验证日志完整性并清理损坏部分
        // 检查校验和并且移除后面的截断部分
        checkAndRemoveTail();
    }

    /**
     * checkAndRemoveTail()是日志系统的核心完整性检查方法，
     * 负责验证日志完整性并清理损坏的尾部数据。
     * 对校验和进行检查并且移除后面的截断部分
     * <p>
     * 流程就是首先将xCheck初始化为0，然后迭代读取每一条日志，再根据读取的日志不断更新xCheck——即更新校验和，
     * 然后去校验log文件开头的校验和与计算得到的xCheck是否一致。其中涉及的其他函数下面会讨论
     */
    private void checkAndRemoveTail() {
        //将读取指针position重置到文件xchecksum头后（跳过4字节校验和),
        //准备从头开始读取日志记录
        rewind();

        //增量计算：基于前值更新校验和
        //逐条验证：通过internNext()读取每条完整记录
        //自动终止：遇到损坏记录或文件结尾时停止
        //从头开始读取，计算校验和，然后在与文件头的xCheckSum进行比较
        int xCheck = 0;
        while (true) {
            // 读取下一条日志
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            // 计算当前这条日志的校验和
            xCheck = calChecksum(xCheck, log);
        }
        //比较计算值与存储的全局校验和,不匹配说明日志存在损坏
        if (xCheck != xChecksum) {
            Panic.panic(Error.BAD_LOG_FILE_EXCEPTION);
        }
        try {
            //将文件截断到最后有效位置
            //截断后面的部分
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            //重新定位文件指针
            randomAccessFile.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //重置读取状态
        //重置文件指针位置
        rewind();
    }

    /**
     * 负责安全读取并验证下一条日志记录。
     * 简单来说就是在解析每条日志的结构并且计算该条日志的校验并进行判断
     *
     * @return
     */
    private byte[] internNext() {
        // 文件边界检查
        // OF_DATA：记录头大小（8字节=4长度+4校验和）
        // 确保有足够空间读取记录头,防止文件末尾越界访问
        // 如果当前文件指针位置 + 8字节 大于等于了文件大小，直接返回
        // 这里8字节是因为每条日志内部的前边4个字节是size，接着4个字节是检验和，再往后才是数据
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        //读取记录长度,分配4字节缓冲区，精确定位到记录开始位置，读取并解析长度字段
        ByteBuffer temp = ByteBuffer.allocate(4);
        try {
            fileChannel.position(position);
            fileChannel.read(temp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //长度有效性验证
        //这里的size是一个日志数据部分的总长度
        // 注意这里size只是表示后面的data的长度，而不是整个日志条目的长度
        int size = Parser.parseInt(temp.array());
        //如果position+日志数据部分总长度+头部8字节超过fileSize，则直接返回null
        if (position + size + OF_DATA >= fileSize) {
            return null;
        }
        // 读取完整记录
        // 按实际大小分配缓冲区，一次性读取一条日志记录的头8字节和数据部分
        // 读取整条日志记录，包括了前面的8字节数据
        ByteBuffer buffer = ByteBuffer.allocate(OF_DATA + size);
        try {
            fileChannel.position(position);
            fileChannel.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //转换为字节数组方便处理
        byte[] log = buffer.array();
        //校验和验证，checkSum1：重新计算数据部分校验和；checkSum2：读取日志第4到8字节的校验和
        // 根据后面的data的内容去获得一个校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 读取前面的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        //不一致则判定记录损坏
        // 如果校验和不相等，直接返回
        if (checkSum1 != checkSum2) {
            return null;
        }
        //更新位置指针，移动到下条记录起始位置，即position加上当前日志的总长度
        position += log.length;
        return log;
    }

    /**
     * 日志系统中用于计算数据校验和的核心方法，采用了一种高效且实用的校验算法。
     *
     * @param xCheck 初始校验值（通常为0或前次计算结果）
     * @param log    需要计算校验和的字节数组
     * @return 计算出的32位整数校验和
     * 实际上就是根据一个种子不断的重复计算
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            //SEED选择质数13331（16进制0x3413）
            //当前字节（自动提升为int）
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }


    /**
     * 更新日志文件的校验和
     * 增量更新日志文件的全局校验和
     * 将新校验和立即持久化到文件头
     * 更新校验和updateXCheckSum
     * 通过上面的逻辑也可以看出来，
     * 其实就是在原来的全局校验和的基础上再与新的日志条目进行一次校验和计算，然后将新的结果写在log文件的前4字节
     *
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        //基于当前校验和和新日志记录计算新值,保持校验连续性
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            //定位到文件头（position 0）
            fileChannel.position(0);
            //写入4字节校验和
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            //强制刷盘（metadata不刷）
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 包装日志记录
     * 将原始数据包装成完整日志记录格式,添加长度头和校验和
     * 一个完整的日志记录格式
     * | 长度(4) | 校验和(4) | 数据(N)   |
     * <p>
     * 将数据包裹为日志条目wrapLog
     * 填充前面的8字节元信息即可
     *
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        //首先对数据部分计算校验和
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        //序列号长度
        byte[] size = Parser.int2Byte(data.length);
        //拼接数据：长度+校验和+数据
        return Bytes.concat(size, checksum, data);
    }


    /**
     * log()方法是日志系统的核心写入方法，负责原子性地记录事务日志。
     * <p>
     * <p>
     * 记录日志是在log文件中顺序一直往后写的，因此这里也在最后调用了updateXCheckSum，
     * 将当前的校验和与新的日志进行计算更新
     *
     *
     * @param data 要记录的原始事务数据（字节数组）
     *             示例：
     *             原始数据: [0x01, 0x02]
     *             包装后: [0x00,0x00,0x00,0x02, 0x00,0x00,0x34,0x15, 0x01,0x02]
     */
    @Override
    public void log(byte[] data) {
        //调用 wrapLog()方法添加记录头 格式：[4字节长度][4字节校验和][实际数据]
        // 将数据包装成日志条目，也就是添加了长度头和校验和
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            //定位到文件末尾（原子操作）
            //写入完整日志记录
            //使用NIO提升IO性能
            //写入日志
            fileChannel.position(fileChannel.size());
            fileChannel.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        //增量更新日志文件的校验和
        //立即持久化到文件头
        //更新校验和
        updateXChecksum(log);
    }

    /**
     * 简单来说就是在解析每条日志的结构并且计算校验和进行判断
     *
     * @param x 其中x对应的就是log文件中需要截断的位置（字节数）
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(x);

        } finally {
            lock.unlock();
        }
    }

    /**
     * next()方法是日志系统的核心读取接口，负责安全地读取下一条有效日志记录。
     * 向外提供的获取下一条日志的接口Next
     * 这里和internNext的区别就是只返回的是每一个日志的Data部分，而不考虑前边8字节的元信息
     *
     * @return 成功：日志记录的数据部分（不含头部）
     * 失败：null（文件末尾或损坏记录）
     * 示例：
     * 完整记录: [0,0,0,2, 0,0,0x12,0x34, 0xAA,0xBB]
     * 返回数据: [0xAA,0xBB]
     */
    @Override
    public byte[] next() {
        byte[] result;
        lock.lock();
        try {
            //调用internNext()读取下一条完整记录
            byte[] log = internNext();
            if (log == null) {
                result = null;
            } else {//OF_DATA常量值为8（4长度+4校验和），只返回原始数据部分（剥离头部）
                result = Arrays.copyOfRange(log, OF_DATA, log.length);
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    /**
     * 用于重置日志读取位置的关键方法
     * 将内部读取指针 position重置为 4
     * 使后续的 next()调用从日志文件的第一条记录开始读取,
     * 因为日志文件的前4字节存储全局校验和
     * 实际日志记录从第4字节后开始存储
     * 这种设计使得：
     * 字节0-3：全局校验和（xChecksum）
     * 字节4+：日志记录数据
     * Rewind 将文件指针位置重新定位到最开始的文件校验和后面，即4字节的位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
