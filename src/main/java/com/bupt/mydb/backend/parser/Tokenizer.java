package com.bupt.mydb.backend.parser;

import com.bupt.mydb.common.Error;

/**
 * @author gao98
 * date 2025/9/24 12:20
 * description:
 * 为了完成SQL的解析，需要一个分词器，分词的原则就是以空格作为区分，将语句切分为多个token
 * Tokenizer是 SQL 解析器的词法分析组件，负责将原始 SQL 语句转换为可处理的词法单元（token）。
 * 这里的token分词采用的本质上是逐层判断的过程，没有使用状态机，
 * 如果有时间后续可能考虑改进。但这毕竟不是本次数据库项目的核心，因此也不过多展开
 */
public class Tokenizer {
    // 输入的SQL语句
    private byte[] stat;
    // 当前解析位置
    private int pos;
    // 当前token
    private String currentToken;
    // 标记是否需要刷新当前token
    private boolean flushToken;
    // 解析过程中发生的异常
    private Exception err;

    // NewTokenizer 构造函数，初始化输入的SQL语句
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 预读下一个token但不移动指针，实现了惰性解析和错误处理机制。
     * Peek 返回当前token，如果有错误抛出异常
     *
     * @return
     * @throws Exception
     */
    public String peek() throws Exception {
        // 快速失败
        if (err != null) {
            throw err;
        }
        // 如果需要刷新token，则调用NextToken方法
        if (flushToken) {
            String token = null;
            try {
                // 实际解析逻辑
                token = next();
            } catch (Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * Pop 将当前的标记设置为需要刷新，这样下次调用peek()时会生成新的标记
     */
    public void pop() {
        flushToken = true;
    }

    // ErrStat 返回带有错误位置标记的输入状态
    public byte[] errStat() {
        byte[] res = new byte[stat.length + 3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        return res;
    }

    // popByte 移动到下一个字节
    private void popByte() {
        pos++;
        if (pos > stat.length) {
            pos = stat.length;
        }
    }

    // peekByte 查看当前字节，不移动位置
    private Byte peekByte() {
        if (pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    // next 获取下一个token，如果有错误抛出异常
    private String next() throws Exception {
        if (err != null) {
            throw err;
        }
        return nextMetaState();
    }

    // nextMetaState 获取下一个元状态。元状态可以是一个符号、引号包围的字符串或者一个由字母、数字或下划线组成的标记
    private String nextMetaState() throws Exception {
        while (true) {
            Byte b = peekByte();
            // 如果没有下一个字节，返回空字符串
            if (b == null) {
                return "";
            }
            // 如果下一个字节不是空白字符，跳出循环
            if (!isBlank(b)) {
                break;
            }
            popByte();
        }
        // 获取下一个字节
        byte b = peekByte();
        if (isSymbol(b)) {
            // 如果这个字节是一个符号，跳过这个字节
            popByte();
            // 并返回这个符号
            return new String(new byte[]{b});
        } else if (b == '"' || b == '\'') {
            // 如果这个字节是引号，获取下一个引号状态
            return nextQuoteState();
        } else if (isAlphaBeta(b) || isDigit(b)) {
            // 如果这个字节是字母、数字或下划线，获取下一个标记状态
            return nextTokenState();
        } else {
            // 否则，设置错误状态为无效的命令异常
            err = Error.INVALID_COMMAND_EXCEPTION;
            throw err;
        }
    }

    // nextTokenState 获取下一个标记。标记是由字母、数字或下划线组成的字符串。
    private String nextTokenState() throws Exception {
        // 创建一个buffer，用于存储标记
        StringBuilder sb = new StringBuilder();
        while (true) {
            // 获取下一个字节
            Byte b = peekByte();
            // 如果没有下一个字节，或者下一个字节不是字母、数字或下划线，那么结束循环
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                // 如果下一个字节是空白字符，那么跳过这个字节
                if (b != null && isBlank(b)) {
                    popByte();
                }
                // 返回标记
                return sb.toString();
            }
            // 如果下一个字节是字母、数字或下划线，那么将这个字节添加到buffer中
            sb.append(new String(new byte[]{b}));
            // 跳过这个字节
            popByte();
        }
    }

    //判断当前字节是否是数字
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    //判断当前字节是否是字母
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    // nextQuoteState 处理引号状态，即处理被引号包围的字符串。
    private String nextQuoteState() throws Exception {
        // 获取下一个字节，这应该是一个引号
        byte quote = peekByte();
        // 跳过这个引号
        popByte();
        // 创建一个buffer，用于存储被引号包围的字符串
        StringBuilder sb = new StringBuilder();
        while (true) {
            // 获取下一个字节
            Byte b = peekByte();
            if (b == null) {
                // 如果没有下一个字节，设置错误状态为无效的命令异常
                err = Error.INVALID_COMMAND_EXCEPTION;
                throw err;
            }
            if (b == quote) {
                // 如果这个字节是引号，跳过这个字节，并跳出循环
                popByte();
                break;
            }
            // 如果这个字节不是引号，将这个字节添加到StringBuilder中
            sb.append(new String(new byte[]{b}));
            // 跳过这个字节
            popByte();
        }
        return sb.toString();
    }

    // IsSymbol 判断一个字节是否是符号
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    // IsBlank 判断一个字节是否是空白字符
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
