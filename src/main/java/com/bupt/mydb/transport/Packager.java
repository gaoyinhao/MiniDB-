package com.bupt.mydb.transport;

/**
 * @author gao98
 * date 2025/9/24 10:32
 * description:
 * Packager是数据库网络通信的高层封装，
 * 负责在网络传输基础上增加协议编解码功能。
 * Packager中负责调用Package和Transporter的api，
 * 用来做实际的数据发送和接收
 */
public class Packager {
    //底层字节传输
    private Transporter transporter;
    //协议编解码器
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        //将对象转为字节
        byte[] data = encoder.encode(pkg);
        // 将字节转为十六进制字符串进行传输
        transporter.send(data);
    }

    public Package receive() throws Exception {
        // 接收十六进制数据饼转为字节数据
        byte[] data = transporter.receive();
        // 将字节转为Package对象
        return encoder.decode(data);
    }

    //关闭流
    public void close() throws Exception {
        transporter.close();
    }
}
