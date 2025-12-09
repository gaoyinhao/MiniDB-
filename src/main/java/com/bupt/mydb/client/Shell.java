package com.bupt.mydb.client;

import java.util.Scanner;

/**
 * @author gao98
 * date 2025/9/24 14:00
 * description:
 * 需要给用户提供一个简易的shell界面，只需要不断的打印即可，同时在每次接收输入后发送请求等待响应
 */
// Shell 用于接受用户的输入，并调用Client.execute()
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        // 用于读取用户的输入
        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.println(":> ");
                String statStr = scanner.nextLine();
                if ("exit".equals(statStr) || "quit".equals(statStr)) {
                    break;
                }
                try {
                    byte[] res = client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            scanner.close();
            client.close();
        }
    }
}
