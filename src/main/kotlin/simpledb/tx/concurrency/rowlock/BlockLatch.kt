package simpledb.tx.concurrency.rowlock

import simpledb.file.BlockId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class BlockLatch {
    private val latches: ConcurrentHashMap<BlockId, ReadWriteLock> = ConcurrentHashMap()

    fun latches(): Map<BlockId, ReadWriteLock> = latches

    private fun getLatch(blk: BlockId): ReadWriteLock {
        val latch = latches.putIfAbsent(blk, ReentrantReadWriteLock())
        if (latch != null) {
            return latch
        }
        return latches[blk]!!
    }

    fun rLatch(blk: BlockId) {
        val latch = getLatch(blk)
        latch.readLock().lock()
    }

    fun wLatch(blk: BlockId) {
        val latch = getLatch(blk)
        latch.writeLock().lock()
    }

    fun rUnlatch(blk: BlockId) {
        val latch = getLatch(blk)
        latch.readLock().unlock()
    }

    fun wUnlatch(blk: BlockId) {
        val latch = getLatch(blk)
        latch.writeLock().unlock()
    }
}
