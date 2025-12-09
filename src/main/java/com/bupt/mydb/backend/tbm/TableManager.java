package com.bupt.mydb.backend.tbm;


import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.parser.statement.*;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.backend.vm.VersionManager;

/**
 * 从前几章其实可以看出，实现各层模块的过程就是先定义一个抽象的数据结构，
 * 然后再实现它的管理器，而这个管理器就是操作这个抽象的数据。
 * 而这一层又会调用它下一层的api，这些api内部执行的操作的就是下一层内部抽象出来的数据结构
 * 本节实现的表管理器就是来操作字段（Field）与表（Table）的
 * 注意SimpleDB不支持多数据库，也就是说所有的表都存在于同一个db文件中，只有一个数据库
 * 每次创建表时使用头插法，即每次创建表时将新表插入到链表的头部，因此每次创建新表时需要更新记录第一个表的uid
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
