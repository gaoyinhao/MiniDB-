package com.bupt.mydb.client;

import com.bupt.mydb.transport.Package;
import com.bupt.mydb.transport.Packager;

/**
 * @author gao98
 * date 2025/9/24 14:00
 * description:
 * 最后要完成的部分是完成客户端和服务端的交互，
 * 从而实现用户发送指令，服务端接收指令，解析并执行操作并返回结果给客户端的流程
 * 这一节先讨论服务端的实现
 * 真正执行网络相关请求的是Client
 */
public class Client {
    // rt RoundTripper实例，用于处理请求的往返传输
    private RoundTripper roundTripper;

    // NewClient 接收一个Packager对象作为参数，并创建一个新的RoundTripper实例
    public Client(Packager packager) {
        this.roundTripper = new RoundTripper(packager);
    }

    // Execute 接收一个字节数组作为参数，将其封装为一个Package对象，并通过RoundTripper发送
    // 如果响应的Package对象中包含错误，那么抛出这个错误
    // 否则，返回响应的Package对象中的数据
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = roundTripper.roundTrip(pkg);
        if (resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            roundTripper.close();
        } catch (Exception e) {

        }
    }

}
