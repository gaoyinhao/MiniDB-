package com.bupt.mydb.transport;

import com.bupt.mydb.common.Error;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/24 10:31
 * description:
 * Encoder是数据库通信协议中的编解码器，
 * 负责在二进制数据和Package对象之间进行转换。
 * | 类型 | 数据内容             |
 * |(1B) | (变长)              |
 * 正常数据
 * <p>
 * [0][有效数据]
 * <p>
 * 0x00 0x01 0x02...
 * <p>
 * 错误数据
 * <p>
 * [1][错误信息]
 * <p>
 * 0x01 0x45 0x72...
 * Encoder的作用说白了就是完成上面Flag信息的包装和解析
 */

public class Encoder {
    // Encode 根据pkg，添加对应的Flag信息，返回编码后的数据
    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            // 错误包编码流程
            Exception err = pkg.getErr();
            String msg = "Intern Server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            // 正常包编码流程
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    // Decode 根据data，解析出对应的Package
    public Package decode(byte[] data) throws Exception {
        //最小长度校验：至少1字节类型标记
        if (data.length < 1) {
            throw Error.INVALID_PKG_DATA_EXCEPTION;
        }
        //0x00：提取数据部分构造正常Package
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            //0x01：构造携带错误信息的Package
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            //非法类型抛出异常
            throw Error.INVALID_PKG_DATA_EXCEPTION;
        }
    }
}
