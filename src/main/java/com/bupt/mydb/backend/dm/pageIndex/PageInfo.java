package com.bupt.mydb.backend.dm.pageIndex;

/**
 * @author gao98
 * date 2025/9/19 22:14
 * description:
 * PageInfo用来表示搜索得到的页面的信息，包括了页号和该页中的空闲空间大小（字节为单位）
 */
public class PageInfo {
    //页面编号,通常从1开始
    private int pgno;
    //空闲空间,PAGE_SIZE - usedSpace
    private int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }

    public int getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(int freeSpace) {
        this.freeSpace = freeSpace;
    }

    public int getPgno() {
        return pgno;
    }

    public void setPgno(int pgno) {
        this.pgno = pgno;
    }
}
