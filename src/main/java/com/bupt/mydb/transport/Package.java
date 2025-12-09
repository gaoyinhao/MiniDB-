package com.bupt.mydb.transport;

/**
 * @author gao98
 * date 2025/9/24 10:32
 * description:
 * Package类是数据库网络通信中的数据传输单元，
 * 作为客户端与服务端之间的通用数据传输载体。
 * 使用Package来表示服务端与客户端交换的数据
 *
 * 发送消息时的规定如下：Flag``Data
 *
 * 若Flag为0，那么发送的是数据，Data就是数据
 * 若Flag为1，那么发送的是错误信息，Data为空，Err是错误信息
 */
public class Package {
    //    存储实际传输的二进制数据
    //例如：SELECT结果序列化
    private byte[] data;

    //    携带通信过程中的异常信息
    //例如：new IOException("Connection reset")
    private Exception err;

    /**
     * 双通道设计
     * // 成功响应通道
     * new Package(responseData, null);
     * <p>
     * // 错误响应通道
     * new Package(null, new SQLException("Syntax error"));
     *
     * @param data
     * @param err
     */
    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
