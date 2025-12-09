package com.bupt.mydb.client;

import com.bupt.mydb.transport.Packager;
import com.bupt.mydb.transport.Package;

/**
 * @author gao98
 * date 2025/9/24 13:56
 * description:
 * 是数据库客户端网络通信的核心组件，
 * 负责管理完整的请求-响应循环。
 * 客户端是主动向服务端发起请求的，每次发起后还要等待一个响应，
 * 为此对客户端来说，每次的请求都是一去一回，RoundTripper就是完成这个工作的
 */
// RoundTripper 用于发送请求并接受响应
public class RoundTripper {
    //协议打包解包器
    private Packager packager;


    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    //执行完整通信周期
    // RoundTrip 用于处理请求的往返传输
    public Package roundTrip(Package pkg) throws Exception {
        // 发送请求包
        packager.send(pkg);
        // 接收响应包，并返回
        return packager.receive();
    }

    //释放资源
    public void close() throws Exception {
        packager.close();
    }
}
