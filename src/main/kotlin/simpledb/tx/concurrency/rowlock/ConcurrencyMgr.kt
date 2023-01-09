package simpledb.tx.concurrency.rowlock

import simpledb.file.BlockId
import simpledb.record.RID
import simpledb.tx.rowlock.TransactionImpl
import simpledb.tx.concurrency.rowlock.LockTable as LockTableRowLock

class ConcurrencyMgr {
    private val locks: MutableMap<BlockId, String> = mutableMapOf()
    fun release(tx: TransactionImpl) {
        for ((blk, mode) in locks) {
            if (mode == "S") blockLatch.rUnlatch(blk)
            else blockLatch.wUnlatch(blk)
        }
        locks.clear()
        lockTblRowLock.unlock(tx, tx.sharedLockRIDs + tx.exclusiveLockRIDs)
    }

    fun rLatchPage(blk: BlockId) {
        blockLatch.rLatch(blk)
        locks[blk] = "S"
    }

    fun rUnlatchPage(blk: BlockId) {
        if (locks[blk] == "S") {
            blockLatch.rUnlatch(blk)
            locks.remove(blk)
        }
    }

    fun wLatchPage(blk: BlockId) {
        blockLatch.wLatch(blk)
        locks[blk] = "X"
    }

    fun wUnlatchPage(blk: BlockId) {
        if (locks[blk] == "X") {
            blockLatch.wUnlatch(blk)
            locks.remove(blk)
        }
    }

    fun lockExclusive(tx: TransactionImpl, rid: RID) {
        lockTblRowLock.lockExclusive(tx, rid)
    }

    fun lockShared(tx: TransactionImpl, rid: RID) {
        lockTblRowLock.lockShared(tx, rid)
    }

    companion object {
        private val lockTblRowLock = LockTableRowLock()
        private val blockLatch = BlockLatch()
    }
}
