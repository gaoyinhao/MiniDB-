package com.bupt.mydb.backend.tbm;


import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.parser.statement.*;
import com.bupt.mydb.backend.utils.Parser;
import com.bupt.mydb.backend.vm.VersionManager;
import com.bupt.mydb.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {
    // 版本管理器，用于管理事务的版本
    VersionManager vm;
    // 数据管理器，用于管理数据的存储和读取
    DataManager dm;
    // 启动信息管理器，用于管理数据库启动信息
    private Booter booter;
    // 表缓存，用于缓存已加载的表，键是表名，值是表对象
    private Map<String, Table> tableCache;
    // 事务表缓存，用于缓存每个事务修改过的表，键是事务ID，值是表对象列表
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    // loadTables 加载所有的数据库表
    private void loadTables() {
        // 获取第一个表的UID
        long uid = firstTableUid();
        // 当UID不为0时，表示还有表需要加载
        while (uid != 0) {
            // 加载表，并获取表的UID
            Table tb = Table.loadTable(this, uid);
            // 更新UID为下一个表的UID
            uid = tb.nextUid;
            // 将加载的表添加到表缓存中
            tableCache.put(tb.name, tb);
        }
    }

    // firstTableUid 获取 Booter 文件的前八位字节
    //首先要通过booter读取第一个表的uid，读取第一个表之后后面的都可以轮流读取
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    //begin语句处理
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    //commit语句处理
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    //abort语句处理
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    //show语句处理
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if (t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    //create语句处理
    //在创建数据表时由于是头插法，因此要更新启动信息，其中对应的updateFirstTableUid：
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            // 如果表已经存在，则返回错误
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DUPLICATED_TABLE_EXCEPTION;
            }
            // 创建表
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            // 更新第一个数据表的Uid
            updateFirstTableUid(table.uid);
            // 将表添加到表缓存中
            tableCache.put(create.tableName, table);
            // 将表添加到事务表缓存中
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }
    //insert语句处理
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TABLE_NOT_FOUND_EXCEPTION;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }
    //select语句处理
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TABLE_NOT_FOUND_EXCEPTION;
        }
        return table.read(xid, read).getBytes();
    }
    //update语句处理
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TABLE_NOT_FOUND_EXCEPTION;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }
    //delete语句处理
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TABLE_NOT_FOUND_EXCEPTION;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}
