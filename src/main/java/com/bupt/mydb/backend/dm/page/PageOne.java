package com.bupt.mydb.backend.dm.page;

import com.bupt.mydb.backend.dm.pageCache.PageCache;
import com.bupt.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @author gao98
 * date 2025/9/19 22:14
 * description:
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 * PageOne是数据库系统中用于管理第一页特殊结构的工具类，
 * 主要负责版本控制信息(Version Control, VC)的维护。
 * <p>
 * +----------------+----------------+
 * | 打开VC(8字节)   | 关闭VC(8字节)   |
 * +----------------+----------------+
 * 100              108              116
 * 打开VC：数据库打开时设置的随机值
 * 关闭VC：数据库关闭时从打开VC复制而来
 * 正常关闭：打开VC == 关闭VC
 * 异常关闭：打开VC != 关闭VC
 *
 * 数据库中db文件的的第一页用于文件的校验（基本上所有文件都需要校验这一层，防止出现问题）
 * 校验的规则当然是自定义的，校验的规则一定要满足：在开始创建时做什么什么操作，
 * 然后在结束操作时做另外一个操作，且和创建时做的操作要对应校验
 *这里校验的规则如下：
 *
 * 第一页Page的大小也遵循和其他普通的页一样，当前的默认是8KB
 * 在每次启动数据库的时候，生成一段随机字节（这里选择的是8字节），存储在100-107字节处
 * 在数据库关闭时，将100-107处的字节复制到108-115字节处
 * 下一次启动数据库时，校验这两部分的字节是否一致
 *
 */
public class PageOne {
    // 版本控制信息在页面中的偏移量
    // PageOneOffsetValidCheck 用于数据库文件中第一页的检查，偏移量为100的位置后的8+8个字节用来校验
    private static final int OF_VC = 100;
    // 版本控制信息的长度
    // PageOneLengthValidCheck 100字节后两个8字节用来校验，成功的情况下两个8字节应该一致
    private static final int LEN_VC = 8;

    /**
     * 创建并初始化第一页的数据
     *
     * @return
     */
    public static byte[] InitRaw() {
        // 创建以个新页面，使用全零初始化
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 设置初始VC，立即设置开放版本控制
        setVcOpen(raw);
        return raw;
    }

    /**
     * 设置新的打开VC值
     * 设置校验状态为打开，即打开数据库时的状态
     * @param pg
     */
    public static void setVcOpen(Page pg) {
        // 标记页面为脏
        pg.setDirty(true);
        // 委托给字节数组版本
        setVcOpen(pg.getData());
    }

    /**
     * 生成8字节随机数，复制到页面指定位置(偏移100)
     * 即在100-107这八个字节位置存放一个八字节随机数
     * @param raw
     */
    private static void setVcOpen(byte[] raw) {
        // 生成校验数据
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 设置校验状态为关闭，即关闭数据库时的状态
     *
     * @param pg
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(false);
        setVcClose(pg.getData());
    }

    /**
     * 将校验数据复制到后半部分的字节
     * 不生成新随机数，简单内存拷贝，同样标记页面为脏
     * 将100-107位置的随机数copy到108-115位置
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * checkVc()方法是数据库系统中用于验证数据库是否正常关闭的关键方法，
     * 通过检查第一页中的版本控制(VC)信息来判断数据库状态。
     * 返回值：
     * true：正常关闭
     * false：异常关闭需要恢复
     * 公开方法：接收Page对象，供外部调用
     *
     *
     *校验数据库文件中第一页的数据
     * @param pg
     * @return
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 私有方法：实际执行校验逻辑，接收原始字节数组
     * 判断字节数组中100-107位置的数据和108-115位置的数据是否相等
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC));
    }
}
