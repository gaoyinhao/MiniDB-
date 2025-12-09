package com.bupt.mydb.backend.im;

import com.bupt.mydb.backend.common.SubArray;
import com.bupt.mydb.backend.dm.dataitem.DataItem;
import com.bupt.mydb.backend.tm.TransactionManagerImpl;
import com.bupt.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gao98
 * date 2025/9/23 15:41
 * description:
 * B+树由一个个的Node组成，每个Node存储在一个DataItem中，其结构如下：
 * [LeafFlag][KeyNumber][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * <p>
 * | LeafFlag | KeyNum| Sibling|Data0|Key0 |Data1|Key1 |Data2|MAX |
 * | (1byte)  |(2byte)| (8byte)|(8b) |(8B)|(8B) |(8B)|(8B) |(var)|
 * <p>
 * LeafFlag：标记该节点是否为叶子节点
 * KeyNumber：该节点中的key的个数
 * SiblingUid：其兄弟节点的uid，用于实现节点的连接
 * SonN KeyN：后续来回穿插的子节点，最后一个Key始终为MAX_VALUE，方便查找
 * 注意这里的uid对应的全都是DataItem的uid，即64位的数字，其中包含了页号和页内偏移两部分信息
 */
public class Node {
    // IsLeftOffset 是否是叶子节点
    static final int IS_LEAF_OFFSET = 0;
    // NumberKeysOffset key个数的偏移位置
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    // SiblingOffset 兄弟节点的偏移位置
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    // NodeHeaderSize 节点数据头部偏移位置
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;
    // BalanceNumber 节点的平衡因子的常量，一个节点最多可以包含32个key
    static final int BALANCE_NUMBER = 32;
    // NodeSize 节点大小，一个节点最多可以包含32个key和32个Son(从0-32其实是33个，所以后面要加2)，每个key和Son各占用8个字节
    //为什么是 BALANCE_NUMBER * 2 + 2？
    // BALANCE_NUMBER：正常容量（32个键）
    // * 2：分裂时临时超载（最多存64个键）
    //+ 2：安全裕度（防止计算误差）
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    // Node B+树的节点表示
    BPlusTree tree;
    //由于其是存储在DataItem的，因此保留了一个DataItem的引用
    DataItem dataItem;
    SubArray raw;
    long uid;

    // SetRawIsLeaf 设置是否为叶子节点，1表示是叶子节点，0表示非叶子节点
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    // GetRawIsLeaf 判断是否是叶子节点
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    // SetRawNumberKeys 设置节点个数
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    // GetRawNumberKeys 获取节点个数
    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    // SetRawSibling 设置兄弟节点的UID，占用8字节
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    // GetRawSibling 获取兄弟节点的UID
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    // SetRawKthSon 设置第kth个子节点的UID 注意k是从0开始的
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    // GetRawKthSon 获取第k个子节点的UID 注意k是从0开始的
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    // SetRawKthKey 设置第k个key的值 注意k是从0开始的
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    // GetRawKthKey 获取第k个key的值 注意k是从0开始的
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    // CopyRawFromKth 从一个节点的原始字节数组中复制一部分数据到另一个节点的原始字节数组中
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        // 将源节点的原始字节数组中的数据复制到目标节点的原始字节数组中
        // 复制的数据包括从起始位置到源节点的原始字节数组的末尾的所有数据
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    // ShiftRawKth 将一个节点的原始字节数组中值从kth开始整体向后移动
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    // NewRootRaw 创建一个新的根节点的原始字节数组
    // 这个新的根节点包含两个子节点，它们的键分别是key和MaxInt64，UID分别是left和right
    static byte[] newRootRaw(long left, long right, long key) {
        // 创建一个新的字节数组，大小为节点的大小
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置节点为非叶子节点
        setRawIsLeaf(raw, false);
        // 设置节点的key的数量为2
        setRawNoKeys(raw, 2);
        // 设置节点的兄弟节点的UID为0
        setRawSibling(raw, 0);
        // 设置第0个子节点的UID为left
        setRawKthSon(raw, left, 0);
        // 设置第0个键的值为key
        setRawKthKey(raw, key, 0);
        // 设置第1个子节点的UID为right
        setRawKthSon(raw, right, 1);
        // 设置第1个键的值为MAX_VALUE
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        // 返回新创建的根节点的原始字节数组
        return raw.raw;
    }

    // NewNilRootRaw 创建一个新的空叶子节点的原始字节数组，这个新的根节点没有子节点和键
    static byte[] newNilRootRaw() {
        // 创建一个新的字节数组，大小为节点的大小
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置节点为叶子节点
        setRawIsLeaf(raw, true);
        // 设置节点的键的数量为0
        setRawNoKeys(raw, 0);
        // 设置节点的兄弟节点的UID为0
        setRawSibling(raw, 0);
        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    //用于在B+树的节点中查找插入下一个节点的位置
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    // SearchNext 在B+树的节点中搜索下一个节点的方法
    // 搜索的逻辑是给定当前的key，要找到当前节点中第一个大于key的已有的key
    public SearchNextRes searchNext(long key) {
        // 获取节点的读锁
        dataItem.rLock();
        try {
            // 创建一个SearchNextRes对象，用于存储搜索结果
            SearchNextRes res = new SearchNextRes();
            // 获取key的总个数
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                // 获取第i个key的值
                long ik = getRawKthKey(raw, i);
                // 如果当前的key大于给定的key，则返回
                if (key < ik) {
                    // 设置子节点的UID
                    res.uid = getRawKthSon(raw, i);
                    // 设置兄弟节点的UID为0
                    res.siblingUid = 0;
                    // 返回搜索结果
                    return res;
                }
            }
            // 如果没有找到下一个节点，设置uid为0
            res.uid = 0;
            // 设置兄弟节点的UID为当前节点的兄弟节点的UID
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    // ============ 用于在B+树中根据key搜索节点 =================
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    // LeafSearchRange 在B+树的叶子节点中搜索一个范围的key
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            // 获取节点中的key的数量
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            // 找到第一个大于或等于左键的键
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            // 创建一个列表，用于存储所有在键值范围内的子节点的UID
            List<Long> uids = new ArrayList<>();
            // 遍历所有的键，将所有小于或等于右键的键对应的子节点的UID添加到列表中
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    //raw中第kth个uid添加到uids中
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            // 如果所有的键都被遍历过，获取兄弟节点的UID
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            // 创建一个LeafSearchRangeRes对象，用于存储搜索结果
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    // ============ 用于在B+树的节点中插入一个节点，并在需要时分裂节点 =================
    class InsertAndSplitRes {
        // 兄弟节点UID（插入时使用）
        // 插入时节点已满但未分裂
        long siblingUid;
        // 分裂产生的新节点UID
        //节点分裂后产生的新节点
        long newSon;
        // 分裂产生的新节点最小键
        //新节点的第一个键值
        long newKey;
    }

    // InsertAndSplit 在B+树的节点中插入一个键值对（uid:key），并在需要时分裂节点
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        // 创建一个标志位，用于标记插入操作是否成功
        boolean success = false;
        // 创建一个异常对象，用于存储在插入或分裂节点时发生的异常
        Exception err = null;
        // 创建一个InsertAndSplitRes对象，用于存储插入和分裂节点的结果
        InsertAndSplitRes res = new InsertAndSplitRes();
        // 在数据项上设置一个保存点，开始一个原子操作，记录当前数据项状态，便于回滚
        dataItem.before();
        try {
            // 尝试在节点中插入键值对，并获取插入结果
            success = insert(uid, key);
            // 如果发生错误或插入失败，回滚数据项的修改
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            // 如果需要分裂节点
            if (needSplit()) {
                try {
                    // 分裂节点，并获取分裂结果
                    SplitRes r = split();
                    // 设置新节点的UID和新键，并返回结果
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                // 如果不需要分裂节点，提交数据项的修改
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 如果发生错误，回滚数据项的修改
                dataItem.unBefore();
            }
        }
    }

    // insert 在B+树的节点中插入一个键值对的方法
    private boolean insert(long uid, long key) {
        // 获取节点中的键的数量
        int noKeys = getRawNoKeys(raw);
        // 初始化插入位置的索引
        int kth = 0;
        // 找到第一个大于或等于要插入的键的键的位置
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        // 如果所有的键都被遍历过，并且存在兄弟节点，插入失败
        // 这是因为B+树的节点最多只能存储BALANCE_NUMBER个键值对，当节点满时需要分裂
        // 而分裂后，新节点的第一个键值对的键必须大于当前节点的所有键值对的键
        // 因此，如果所有的键都被遍历过，并且存在兄弟节点，说明当前节点的所有键值对的键都小于要插入的键
        // 所以插入失败
        if (kth == noKeys && getRawSibling(raw) != 0) {
            return false;
        }
        // 如果节点是叶子节点
        if (getRawIfLeaf(raw)) {
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth);
            // 在插入位置插入新的键和子节点的UID
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys + 1);
        } else {
            // 如果节点是非叶子节点
            // 获取插入位置的键
            long kk = getRawKthKey(raw, kth);
            // 在插入位置插入新的键
            setRawKthKey(raw, key, kth);
            // 在插入位置后的所有键和子节点向后移动一位
            shiftRawKth(raw, kth + 1);
            // 在插入位置的下一个位置插入原来的键和新的子节点的UID
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            // 更新节点中的键的数量
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    // needSplit 判断节点是否需要分裂
    // 如果一个节点的key的数量达到BALANCE_NUMBER * 2 需要分裂
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    // split 分裂B+树的节点
    // 当一个节点的键的数量达到 BALANCE_NUMBER * 2 时，就意味着这个节点已经满了，需要进行分裂操作
    // 分裂操作的目的是将一个满的节点分裂成两个节点，每个节点包含一半的键
    private SplitRes split() throws Exception {
        // 创建一个新的字节数组，用于存储新节点的原始数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置新节点的叶子节点标志，与原节点相同
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        // 设置新节点的键的数量为BALANCE_NUMBER
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        // 设置新节点的兄弟节点的UID，与原节点的兄弟节点的UID相同
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 从原节点的原始字节数组中复制一部分数据到新节点的原始字节数组中
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 在数据管理器中插入新节点的原始数据，并获取新节点的UID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        // 更新原节点的键的数量为BALANCE_NUMBER
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 更新原节点的兄弟节点的UID为新节点的UID
        setRawSibling(raw, son);
        // 创建一个SplitRes对象，用于存储分裂结果
        SplitRes res = new SplitRes();
        // 设置新节点的UID
        res.newSon = son;
        // 设置新键为新节点的第一个键的值
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
