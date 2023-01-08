package simpledb.tx.rowlock

import simpledb.buffer.BufferMgr
import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.log.LogMgr
import simpledb.record.RID
import simpledb.tx.BufferList
import simpledb.tx.Transaction
import simpledb.tx.concurrency.rowlock.ConcurrencyMgr
import simpledb.tx.recovery.RecoveryMgr

class TransactionImpl(private val fm: FileMgr, lm: LogMgr?, private val bm: BufferMgr) : Transaction {
    private val recoveryMgr: RecoveryMgr
    private val concurMgr: ConcurrencyMgr
    val txNum: Int = nextTxNumber()
    private val myBuffers: BufferList
    private val mutSharedLockRIDs = mutableListOf<RID>()
    private val mutExclusiveLockRIDs = mutableListOf<RID>()

    val sharedLockIRIDs : List<RID>
        get() = mutSharedLockRIDs.toList()
    val exclusiveLockRIDs : List<RID>
        get() = mutExclusiveLockRIDs.toList()

    init {
        recoveryMgr = RecoveryMgr(this, txNum, lm, bm)
        concurMgr = ConcurrencyMgr()
        myBuffers = BufferList(bm)
    }

    override fun commit() {
        recoveryMgr.commit()
        concurMgr.release(this)
        myBuffers.unpinAll()
        println("transaction $txNum committed")
    }

    override fun rollback() {
        recoveryMgr.rollback()
        concurMgr.release(this)
        myBuffers.unpinAll()
        println("transaction $txNum roll back")
    }

    override fun recover() {
        bm.flushAll(txNum)
        recoveryMgr.recover()
    }

    override fun pin(blk: BlockId) {
        myBuffers.pin(blk)
    }

    override fun unpin(blk: BlockId) {
        myBuffers.unpin(blk)
    }

    override fun getInt(blk: BlockId, offset: Int): Int {
        val buff = myBuffers.getBuffer(blk)
        return buff.contents().getInt(offset)
    }

    override fun getString(blk: BlockId, offset: Int): String {
        val buff = myBuffers.getBuffer(blk)
        return buff.contents().getString(offset)
    }

    override fun setInt(blk: BlockId, offset: Int, `val`: Int, okToLog: Boolean) {
        val buff = myBuffers.getBuffer(blk)
        var lsn = -1
        if (okToLog) lsn = recoveryMgr.setInt(buff, offset, `val`)
        val p = buff.contents()
        p.setInt(offset, `val`)
        buff.setModified(txNum, lsn)
    }

    override fun setString(blk: BlockId, offset: Int, `val`: String, okToLog: Boolean) {
        val buff = myBuffers.getBuffer(blk)
        var lsn = -1
        if (okToLog) lsn = recoveryMgr.setString(buff, offset, `val`)
        val p = buff.contents()
        p.setString(offset, `val`)
        buff.setModified(txNum, lsn)
    }

    override fun size(filename: String): Int {
        val dummyBlk = BlockId(filename, END_OF_FILE)
        concurMgr.rLatchPage(dummyBlk)
        try {
            return fm.length(filename)
        } finally {
            concurMgr.rUnlatchPage(dummyBlk)
        }
    }

    override fun append(filename: String): BlockId {
        val dummyBlk = BlockId(filename, END_OF_FILE)
        concurMgr.wLatchPage(dummyBlk)
        try {
            return fm.append(filename)
        } finally {
            concurMgr.wUnlatchPage(dummyBlk)
        }
    }

    override fun blockSize(): Int {
        return fm.blocksize()
    }

    override fun availableBuffs(): Int {
        return bm.available()
    }

    fun lockShared(blk: BlockId, slot: Int) {
        concurMgr.lockShared(this, getRid(blk, slot))
    }

    fun lockExclusive(blk: BlockId, slot: Int) {
        concurMgr.lockExclusive(this, getRid(blk, slot))
    }

    fun rLatchPage(blk: BlockId) {
        concurMgr.rLatchPage(blk)
    }

    fun rUnlatchPage(blk: BlockId) {
        concurMgr.rUnlatchPage(blk)
    }

    fun wLatchPage(blk: BlockId) {
        concurMgr.wLatchPage(blk)
    }

    fun wUnlatchPage(blk: BlockId) {
        concurMgr.wUnlatchPage(blk)
    }

    private fun getRid(blk: BlockId, slot: Int): RID {
        return RID(blk.number(), slot)
    }

    fun addSharedLockRID(rid: RID) {
        mutSharedLockRIDs.add(rid)
    }

    fun addExclusiveLockRID(rid: RID) {
        mutExclusiveLockRIDs.add(rid)
    }

    companion object {
        private var nextTxNum = 0
        private const val END_OF_FILE = -1

        @Synchronized
        private fun nextTxNumber(): Int {
            nextTxNum++
            println("new transaction: $nextTxNum")
            return nextTxNum
        }
    }
}
