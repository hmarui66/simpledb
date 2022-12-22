package simpledb.tx.concurrency.rowlock

import simpledb.file.BlockId
import simpledb.tx.concurrency.LockTable

class ConcurrencyMgr {
    private val locks: MutableMap<BlockId, String?> = HashMap()
    fun sLock(blk: BlockId) {
        if (locks[blk] == null) {
            locktbl.sLock(blk)
            locks[blk] = "S"
        }
    }

    fun xLock(blk: BlockId) {
        if (!hasXLock(blk)) {
            sLock(blk)
            locktbl.xLock(blk)
            locks[blk] = "X"
        }
    }

    fun release() {
        for (blk in locks.keys) locktbl.unlock(blk)
        locks.clear()
    }

    private fun hasXLock(blk: BlockId): Boolean {
        val locktype = locks[blk]
        return locktype != null && locktype == "X"
    }

    companion object {
        private val locktbl = LockTable()
    }
}
