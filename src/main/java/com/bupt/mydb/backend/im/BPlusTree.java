package com.bupt.mydb.backend.im;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.DataManager;
import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.tm.TransactionManagerImpl;
import com.bupt.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/23 15:40
 * description:
 * IM层使用Index Manager来实现索引管理，为SimpleDB提供基于B+树的聚簇索引。
 * （当前SimpleDB只支持基于索引的查找，不支持全表扫描）
 * IM直接依赖于DM，因此IM操作的不是Entry，而是DataItem。
 * 索引的数据直接插入到DataItem然后持久化到db文件中，不需要经过版本管理
 */
public class BPlusTree {
    //这里的BootUid和BootDataItem对应的是B+树的根节点uid和根节点uid对应的DataItem
    //数据管理接口
    DataManager dm;
    //根节点指针的存储位置
    long bootUid;
    //存储根节点UID的数据项
    DataItem bootDataItem;
    //保护根节点修改的锁
    Lock bootLock;

    // CreateBPlusTree 创建一个B+树，将根节点插入到数据管理器中
    public static long create(DataManager dm) throws Exception {
        //创建空根节点数据
        byte[] rawRoot = Node.newNilRootRaw();
        // 将一个根节点插入到数据管理器中
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        // 将根节点的uid插入到数据管理器中
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    // LoadBPlusTree 从数据管理器中加载一个B+树
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    // rootUid 获取根节点的uid
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    // updateRootUid 更新根节点的uid
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    // searchLeaf 从一个节点开始搜索叶子节点
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        if (isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    // searchNext 从一个节点开始搜索下一个节点
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) {
                return res.uid;
            }
            nodeUid = res.siblingUid;
        }
    }

    // Search 从B+树中搜索一个key.
    // 这里是条件查询时用到的搜索，为了和范围查询共用同样的接口，等值查询其实就是左右边界相等的范围查询
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    // SearchRange 从B+树中搜索一个范围
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        // 要从叶子节点开始搜索，利用了B+树的特点，叶子节点的key是有序的并且可以连起来
        long leafUid = searchLeaf(rootUid, leftKey);
        // 存储结果的数组
        List<Long> uids = new ArrayList<>();
        // 从叶子节点开始顺序查找。叶子节点的key是有序的
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }
    // Insert 向B+树中插入一个键值对
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    //    关闭B+树
    public void close() {
        bootDataItem.release();
    }
}
