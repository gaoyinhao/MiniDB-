package top.gaoyinhao.mydb.backend.tm;

import com.bupt.mydb.backend.tm.TransactionManager;
import jdk.management.resource.internal.inst.FileOutputStreamRMHooks;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author gao98
 * date 2025/9/17 18:06
 * description:
 */
public class TransactionManagerTest {
    static Random random = new SecureRandom();
    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager transactionManager;
    private Map<Long, Byte> transMap;
    private CountDownLatch countDownLatch;

    @Test
    public void testMultiThread() {
        transactionManager = TransactionManager.create("D:/SystemSoftware/GoogleDownload/tranmger_test");
        transMap = new ConcurrentHashMap<>();
        countDownLatch = new CountDownLatch(noWorkers);
        for (int i = 0; i < noWorkers; i++) {
            Runnable runnable = () -> {
                worker();
            };
            new Thread(runnable).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert new File("D:/SystemSoftware/GoogleDownload/tranmger_test.xid").delete();
    }

    private void worker() {
        boolean inTrnas = false;
        long transXID = 0;
        for (int i = 0; i < noWorks; i++) {
            int op = Math.abs(random.nextInt(6));
            if (op == 0) {
                lock.lock();
                if (inTrnas == false) {
                    long xid = transactionManager.begin();
                    transMap.put(xid, (byte) 0);
                    transCnt++;
                    transXID = xid;
                    inTrnas = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch (status) {
                        case 1:
                            transactionManager.commit(transXID);
                            break;
                        case 2:
                            transactionManager.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte) status);
                    inTrnas = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                if (transCnt > 0) {
                    long xid = (random.nextInt(Integer.MAX_VALUE) % transCnt) + 1;
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = transactionManager.isActive(xid);
                            break;
                        case 1:
                            ok = transactionManager.isCommitted(xid);
                            break;
                        case 2:
                            ok = transactionManager.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        countDownLatch.countDown();
    }
}
