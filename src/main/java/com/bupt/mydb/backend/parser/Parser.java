package com.bupt.mydb.backend.parser;

import java.util.ArrayList;
import java.util.List;

import com.bupt.mydb.backend.parser.statement.*;
import com.bupt.mydb.common.Error;

/**
 * @author gao98
 * date 2025/9/24 12:20
 * description:
 * Parser借助Tokenizer完成SQL语句的解析，也就是说外界模块调用的是Parser下的函数
 * Parser需要区分语句的类型，而这里对SQL语句的区分仅是在分词的第一个词来区分（在当下版本已经足够了）
 * Parser区分语句的类型后会得到不同的语句结果，不同的结果对应了不同的结构体（下面会解释）
 */
public class Parser {
    // Parse 解析SQL语句
    public static Object Parse(byte[] statement) throws Exception {
        //先实例化分词器
        Tokenizer tokenizer = new Tokenizer(statement);
        //获取下一个完整的token
        String token = tokenizer.peek();
        //刷新标记
        tokenizer.pop();

        Object stat = null;
        // 如果在解析过程中出现错误，保存错误信息
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    // 如果标记的值不符合预期，抛出异常
                    throw Error.INVALID_COMMAND_EXCEPTION;
            }
        } catch (Exception e) {
            statErr = e;
        }
        try {
            // 获取下一个标记
            String next = tokenizer.peek();
            // 如果还有未处理的标记，那么抛出异常
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        // 如果存在错误，抛出异常
        if (statErr != null) {
            throw statErr;
        }
        // 返回生成的语句对象
        return stat;
    }

    // parseShow 解析show语句
    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw Error.INVALID_COMMAND_EXCEPTION;
    }

    // parseUpdate 解析update语句
    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        // 获取表名
        update.tableName = tokenizer.peek();
        tokenizer.pop();
        // 获取SET关键字
        if (!"set".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取字段名
        update.fieldName = tokenizer.peek();
        tokenizer.pop();
        // 获取等号
        if (!"=".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取字段值
        update.value = tokenizer.peek();
        tokenizer.pop();
        // 获取WHERE子句
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }
        update.where = parseWhere(tokenizer);
        return update;
    }

    // parserDelete 解析delete语句
    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();
        // 获取from关键字
        if (!"from".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取表名
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        delete.tableName = tableName;
        tokenizer.pop();
        // 获取where子句
        delete.where = parseWhere(tokenizer);
        return delete;
    }

    // parseInsert 解析insert语句
    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();
        // 获取into关键字
        if (!"into".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取表名
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        insert.tableName = tableName;
        tokenizer.pop();
        // 获取values关键字
        if (!"values".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        // 获取values的值
        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    // parseSelect 解析select语句
    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        // 如果是*，那么获取所有字段
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            // 否则获取字段名
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw Error.INVALID_COMMAND_EXCEPTION;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);
        // 获取from关键字
        if (!"from".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取表名
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        read.tableName = tableName;
        tokenizer.pop();
        //获取where子句
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    //这边只支持了两个条件的级联，没有支持多个条件的级联
    // parserWhere 解析where子句
    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();
        // 获取where关键字
        if (!"where".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取第一个表达式
        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOp(logicOp)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        where.logicOp = logicOp;
        tokenizer.pop();
        // 获取第二个表达式
        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if (!"".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        return where;
    }

    // parseSingleExpression 解析单个表达式
    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        // 获取字段名
        String field = tokenizer.peek();
        if (!isName(field)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        exp.field = field;
        tokenizer.pop();
        // 获取比较运算符
        String op = tokenizer.peek();
        if (!isCmpOp(op)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        exp.compareOp = op;
        tokenizer.pop();
        // 获取值
        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    //判断当前str是否是比较运算符
    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    //判断当前str是否是and 或者 or
    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    //注意一下，这里给出了drop语句的解析，但是数据库中还没有添加对drop的支持
    // parseDrop 解析drop语句
    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        //获取table关键字
        if (!"table".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取表名
        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    // parseCreate 解析create语句
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        // 获取table关键字
        if (!"table".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();

        Create create = new Create();
        // 获取表名
        String name = tokenizer.peek();
        if (!isName(name)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        create.tableName = name;
        // 循环获取字段名和字段类型
        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            // 获取字段名
            String field = tokenizer.peek();
            if ("(".equals(field)) {
                break;
            }

            if (!isName(field)) {
                throw Error.INVALID_COMMAND_EXCEPTION;
            }

            tokenizer.pop();
            // 获取字段类型
            String fieldType = tokenizer.peek();
            if (!isType(fieldType)) {
                throw Error.INVALID_COMMAND_EXCEPTION;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throw Error.TABLE_NO_INDEX_EXCEPTION;
            } else if ("(".equals(next)) {
                break;
            } else {
                throw Error.INVALID_COMMAND_EXCEPTION;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        // 获取index关键字
        if (!"index".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        // 获取索引
        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            // 获取索引名
            String field = tokenizer.peek();
            if (")".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw Error.INVALID_COMMAND_EXCEPTION;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        return create;
    }

    //判断当前str是否是字段类型
    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    // parseAbort 解析abort语句
    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        // abort语句后不应该有任何其他的标记了
        if (!"".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        return new Abort();
    }

    // parseCommit 解析commit语句
    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        // commit语句后不应该有任何其他的标记了
        if (!"".equals(tokenizer.peek())) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        return new Commit();
    }

    // parseBegin 解析begin语句
    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        // 如果没有isolation关键字，那么直接返回，默认隔离级别为read committed
        if ("".equals(isolation)) {
            return begin;
        }
        // 获取isolation关键字
        if (!"isolation".equals(isolation)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取level关键字
        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
        tokenizer.pop();
        // 获取具体的隔离级别
        String tmp1 = tokenizer.peek();
        // 如果是read committed，那么设置隔离级别为read committed
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.INVALID_COMMAND_EXCEPTION;
                }
                return begin;
            } else {
                throw Error.INVALID_COMMAND_EXCEPTION;
            }
        } else if ("repeatable".equals(tmp1)) {
            // 如果是repeatable read，那么设置隔离级别为repeatable read
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw Error.INVALID_COMMAND_EXCEPTION;
                }
                return begin;
            } else {
                throw Error.INVALID_COMMAND_EXCEPTION;
            }
        } else {
            throw Error.INVALID_COMMAND_EXCEPTION;
        }
    }
    //判断当前str是否是字段名
    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
