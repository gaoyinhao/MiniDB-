package com.bupt.mydb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;

/**
 * @author gao98
 * date 2025/9/24 10:32
 * description:
 * 在发送数据时，原始数据被转换为16进制的数据，并在末尾加上换行符
 */
public class Transporter {
    // Transporter 结构体，负责处理数据的发送和接收
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    // NewTransporter 创建一个新的 Transporter 实例
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new java.io.InputStreamReader(
                socket.getInputStream()));
        this.writer = new BufferedWriter(new java.io.OutputStreamWriter(
                socket.getOutputStream()));
    }

    // Send 发送数据，将数据转换为十六进制字符串后发送
    public void send(byte[] data) throws IOException {
        //二进制转十六进制
        // 将数据编码为十六进制并加上换行符
        String raw = hexEncode(data);
        writer.write(raw);
        // 刷新缓冲区，确保数据被发送
        writer.flush();
    }

    // Receive 接收数据，读取十六进制字符串并解码
    public byte[] receive() throws Exception {
        // 读取一行数据
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        // 十六进制转二进制
        // 解码十六进制字符串
        return hexDecode(line);
    }

    // Close 关闭连接
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
