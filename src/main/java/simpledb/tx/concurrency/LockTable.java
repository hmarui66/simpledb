package simpledb.tx.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_TIME = 10000;

    private final Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXlock(blk) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasXlock(blk))
                throw new LockAbortException();
            int val = getLockVal(blk); // will not be negative
            locks.put(blk, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blk) && !waitingTooLong(timestamp))
                wait(MAX_TIME);
            if (hasOtherSLocks(blk))
                throw new LockAbortException();
            locks.put(blk, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blk) {
        int val = getLockVal(blk);
        if (val > 1)
            locks.put(blk, val - 1);
        else {
            locks.remove(blk);
            notifyAll();
        }
    }

    private boolean hasXlock(BlockId blk) {
        return getLockVal(blk) < 0;
    }

    private boolean hasOtherSLocks(BlockId blk) {
        return getLockVal(blk) > 1;
    }

    private boolean waitingTooLong(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_TIME;
    }

    private int getLockVal(BlockId blk) {
        Integer ival = locks.get(blk);
        return (ival == null) ? 0 : ival;
    }


}
