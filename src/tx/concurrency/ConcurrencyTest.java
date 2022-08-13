package tx.concurrency;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import tx.BufferList;
import tx.Transaction;

import java.io.File;

public class ConcurrencyTest {
    private static FileMgr fm;
    private static LogMgr lm;
    private static BufferMgr bm;

    public static void main(String[] args) {
        fm = new FileMgr(new File("dbdir"), 400);
        lm = new LogMgr(fm, "testfile");
        bm = new BufferMgr(fm, lm, 3);
        A a = new A();
        new Thread(a).start();
        B b = new B();
        new Thread(b).start();
        C c = new C();
        new Thread(c).start();
    }

    static class A implements Runnable {
        public void run() {
            try {
                Transaction txA = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txA.pin(blk1);
                txA.pin(blk2);
                System.out.println("Tx A: request slock 1");
                txA.getInt(blk1, 0);
                System.out.println("Tx A: receive slock 1");
                Thread.sleep(1000);
                System.out.println("Tx A: request slock 2");
                txA.getInt(blk2, 0);
                System.out.println("Tx A: receive slock 2");
                txA.commit();
                System.out.println("Tx A: commit");
            } catch (InterruptedException e) {}
        }
    }

    static class B implements Runnable {
        public void run() {
            try {
                Transaction txB = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txB.pin(blk1);
                txB.pin(blk2);
                System.out.println("Tx B: request xlock 2");
                txB.setInt(blk2, 0, 0, false);
                System.out.println("Tx B: receive xlock 2");
                Thread.sleep(1000);
                System.out.println("Tx B: request slock 1");
                txB.getInt(blk1, 0);
                System.out.println("Tx B: receive slock 1");
                txB.commit();
                System.out.println("Tx B: commit");
            } catch (InterruptedException e) {}
        }
    }

    static class C implements Runnable {
        public void run() {
            try {
                Transaction txC = new Transaction(fm, lm, bm);
                BlockId blk1 = new BlockId("testfile", 1);
                BlockId blk2 = new BlockId("testfile", 2);
                txC.pin(blk1);
                txC.pin(blk2);
                Thread.sleep(500);
                System.out.println("Tx C: request xlock 1");
                txC.setInt(blk1, 0, 0, false);
                System.out.println("Tx C: receive xlock 1");
                Thread.sleep(1000);
                System.out.println("Tx C: request slock 2");
                txC.getInt(blk2, 0);
                System.out.println("Tx C: receive slock 2");
                txC.commit();
                System.out.println("Tx B: commit");
            } catch (InterruptedException e) {}
        }
    }
}
