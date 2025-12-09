package com.bupt.mydb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.bupt.mydb.backend.im.BPlusTree;
import com.bupt.mydb.backend.parser.statement.SingleExpression;
import com.bupt.mydb.backend.tm.TransactionManagerImpl;
import com.bupt.mydb.backend.utils.Panic;
import com.bupt.mydb.backend.utils.ParseStringRes;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.common.Error;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 * 字段由字段名（FieldName）、字段类型（TypeName）、索引UID（IndexUid）组成。这里的uid仍然是页号+页内偏移
 * 字段名与字段类型用一种自定义的字符串类型来存储，这主要是因为他们要以字节形式存储，所以需要知道字符串的长度
 * 自定义的字符串规定如下：StringLength``StringData
 * 字段的类型目前被限制为**int32**、**int64**、**string**
 * 如果字段被索引，那么**IndexUid**指向索引B+树的根节点对应的uid；否则该处值为0
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;

    // LoadField 从持久化存储中加载一个Field对象
    public static Field loadField(Table tb, long uid) {
        // 用于存储从持久化存储中读取的原始字节数据
        byte[] raw = null;
        try {
            // 从持久化存储中读取uid对应的原始字节数据
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            // 如果读取过程中出现异常，调用panic方法处理异常
            Panic.panic(e);
        }
        // 断言raw不为空
        assert raw != null;
        // 创建一个新的Field对象，并调用parseSelf方法解析原始字节数据
        //其中对应的解析函数，作用是将原始的字段字节数据转换为结构体
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    // parseSelf 解析原始字节数组并设置字段名、字段类型和索引
    private Field parseSelf(byte[] raw) {
        // 初始化位置为0
        int position = 0;
        // 解析原始字节数组，获取字段名和下一个位置
        ParseStringRes res = Parser.parseString(raw);
        // 设置字段类型
        fieldName = res.str;
        // 更新位置
        position += res.next;
        // 从新的位置开始解析原始字节数组，获取字段类型和下一个位置
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        // 设置字段类型
        fieldType = res.str;
        // 更新位置
        position += res.next;
        // 从新的位置开始解析原始字节数组，获取索引
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        // 如果索引不为0，说明存在B+树索引
        if (index != 0) {
            try {
                // 加载B+树索引
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 创建一个新的Field对象
     * tb        表对象，Field对象所属的表
     * Xid       事务ID
     * fieldName 字段名
     * fieldType 字段类型
     * indexed   是否创建索引
     */
    // CreateField 创建一个新的Field对象
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        // 检查字段类型是否有效
        typeCheck(fieldType);
        // 创建一个新的Field对象
        Field f = new Field(tb, fieldName, fieldType, 0);
        // 如果需要创建索引
        if (indexed) {
            // 创建一个新的B+树索引
            //tb.tbm：获取表的TableManager实例;(TableManagerImpl)：强制类型转换为实现类
            //((TableManagerImpl) tb.tbm).dm：获取DataManager实例;BPlusTree.create(...)：创建B+树索引
            //long index：返回新创建的B+树索引ID
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);
            // 加载这个B+树索引
            //tb.tbm：获取表的TableManager实例;(TableManagerImpl)：强制类型转换以访问实现类特有方法
            //.dm：获取底层DataManager（数据管理器）;BPlusTree.load(index, dm)：加载指定索引ID的B+树
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            // 设置Field对象的索引
            f.index = index;
            // 设置Field对象的B+树
            f.bt = bt;
        }
        //其中涉及到的持久化函数persistSelf如下
        f.persistSelf(xid);
        return f;
    }

    // persistSelf 将当前Field对象持久化到存储中
    private void persistSelf(long xid) throws Exception {
        // 将字段名转换为字节数组
        byte[] nameRaw = Parser.string2Byte(fieldName);
        // 将字段类型转换为字节数组
        byte[] typeRaw = Parser.string2Byte(fieldType);
        // 将索引转换为字节数组
        byte[] indexRaw = Parser.long2Byte(index);
        // 将字段名、字段类型和索引的字节数组合并，然后插入到持久化存储中
        // 插入成功后，会返回一个唯一的uid，将这个uid设置为当前Field对象的uid
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    // typeCheck 检查字段类型是否合法
    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.INVALID_FIELD_EXCEPTION;
        }
    }

    // IsIndexed 判断字段是否有索引
    public boolean isIndexed() {
        return index != 0;
    }

    // Insert 将value和uid插入到B+树索引中
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    // Search 根据key的范围查找uid
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    // String2Value 将字符串转换为字段值
    public Object string2Value(String str) {
        switch (fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /**
     * 根据值生成对应的key！！（重要）
     * 这个函数需要非常明确是什么作用的
     * 我们知道构建索引是为了提高检索速度，检索时通过B+树的key找到对应的节点，然后找到对应的节点下的数据。
     * 那么string类的字符串如何存入B+树的key从而可以比较呢？
     * 要知道这里B+树中存的都是字节，所以可能没那么好操作，这里就可以做一个映射来实现这个转换：
     *
     * @param key
     * @return
     */
    // Value2UKey 根据value生成一个key，这个是用来构建索引的，对于数字直接转换即可
    // 对于字符串，需要按照某个规则转换为数字，从而使得构建的索引能够比较大小
    public long value2Uid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string":
                uid = Parser.str2Uid((String) key);
                break;
            case "int32":
                int uint = (int) key;
                return (long) uint;
            case "int64":
                uid = (long) key;
                break;
        }
        return uid;
    }

    /**
     * 根据值生成对应的字节！！
     * 这个函数也很重要，要将字符串存入文件中，先要转为字节，
     * 而上面咱们提过字符串的字节形式是一个自定义的方式，即StringLength``StringData，为此有如下函数：
     *
     * @param v
     * @return
     */
    // Value2Raw 将value转换为原始字节数组
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch (fieldType) {
            case "int32":
                raw = Parser.int2Byte((int) v);
                break;
            case "int64":
                raw = Parser.long2Byte((long) v);
                break;
            case "string":
                raw = Parser.string2Byte((String) v);
                break;
        }
        return raw;
    }

    // 用于从原始字节数组中解析字段信息
    class ParseValueRes {
        Object v;
        int shift;
    }

    //    根据字段类型（fieldType）将原始字节数据解析为相应的Java对象
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32":
                //int32类型：解析前4个字节为整数，shift为4
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                //int64类型：解析前8个字节为长整数，shift为8
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                //string类型：使用自定义字符串格式解析（StringLength``StringData），返回字符串和实际解析的字节数
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    // PrintValue 打印字段值，主要用于给用户返回查询结果
    public String printValue(Object v) {
        String str = null;
        switch (fieldType) {
            case "int32":
                str = String.valueOf((int) v);
                break;
            case "int64":
                str = String.valueOf((long) v);
                break;
            case "string":
                str = (String) v;
                break;
        }
        return str;
    }

    // String 用于打印Field对象
    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index != 0 ? ", Index" : ", NoIndex")
                .append(")")
                .toString();
    }

    // CalExp 根据条件查询表达式得到查询的结果
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if (res.right > 0) {
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
}
