package simpledb.tx.concurrency.rowlock

import simpledb.file.BlockId
import simpledb.record.RID
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

    fun rLatchPage(blk: BlockId) {
        blockLatch.rLatch(blk)
    }

    fun rUnlatchPage(blk: BlockId) {
        blockLatch.rUnlatch(blk)
    }

    fun wLatchPage(blk: BlockId) {
        blockLatch.wLatch(blk)
    }

    fun wUnlatchPage(blk: BlockId) {
        blockLatch.wUnlatch(blk)

    }

    fun sLock(rid: RID) {
        TODO("Not yet implemented")
    }

    fun xLock(rid: RID) {
        TODO("Not yet implemented")
    }

    companion object {
        private val locktbl = LockTable()
        private val blockLatch = BlockLatch()
    }
}
