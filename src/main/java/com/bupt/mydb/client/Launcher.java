package com.bupt.mydb.client;

import com.bupt.mydb.transport.Encoder;
import com.bupt.mydb.transport.Packager;
import com.bupt.mydb.transport.Transporter;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.net.Socket;

/**
 * @author gao98
 * date 2025/9/24 14:00
 * description:
 */
public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
