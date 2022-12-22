package simpledb.tx.rowlock

import simpledb.buffer.BufferMgr
import simpledb.file.BlockId
import simpledb.file.FileMgr
import simpledb.log.LogMgr
import simpledb.tx.BufferList
import simpledb.tx.Transaction
import simpledb.tx.concurrency.rowlock.ConcurrencyMgr
import simpledb.tx.recovery.RecoveryMgr

class TransactionImpl(private val fm: FileMgr, lm: LogMgr?, private val bm: BufferMgr) : Transaction {
    private val recoveryMgr: RecoveryMgr
    private val concurMgr: ConcurrencyMgr
    private val txNum: Int = nextTxNumber()
    private val myBuffers: BufferList

    init {
        recoveryMgr = RecoveryMgr(this, txNum, lm, bm)
        concurMgr = ConcurrencyMgr()
        myBuffers = BufferList(bm)
    }

    override fun commit() {
        recoveryMgr.commit()
        concurMgr.release()
        myBuffers.unpinAll()
        println("transaction $txNum committed")
    }

    override fun rollback() {
        recoveryMgr.rollback()
        concurMgr.release()
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
        concurMgr.sLock(blk)
        val buff = myBuffers.getBuffer(blk)
        return buff.contents().getInt(offset)
    }

    override fun getString(blk: BlockId, offset: Int): String {
        concurMgr.sLock(blk)
        val buff = myBuffers.getBuffer(blk)
        return buff.contents().getString(offset)
    }

    override fun setInt(blk: BlockId, offset: Int, `val`: Int, okToLog: Boolean) {
        concurMgr.xLock(blk)
        val buff = myBuffers.getBuffer(blk)
        var lsn = -1
        if (okToLog) lsn = recoveryMgr.setInt(buff, offset, `val`)
        val p = buff.contents()
        p.setInt(offset, `val`)
        buff.setModified(txNum, lsn)
    }

    override fun setString(blk: BlockId, offset: Int, `val`: String, okToLog: Boolean) {
        concurMgr.xLock(blk)
        val buff = myBuffers.getBuffer(blk)
        var lsn = -1
        if (okToLog) lsn = recoveryMgr.setString(buff, offset, `val`)
        val p = buff.contents()
        p.setString(offset, `val`)
        buff.setModified(txNum, lsn)
    }

    override fun size(filename: String): Int {
        val dummyBlk = BlockId(filename, END_OF_FILE)
        concurMgr.sLock(dummyBlk)
        return fm.length(filename)
    }

    override fun append(filename: String): BlockId {
        val dummyBlk = BlockId(filename, END_OF_FILE)
        concurMgr.xLock(dummyBlk)
        return fm.append(filename)
    }

    override fun blockSize(): Int {
        return fm.blocksize()
    }

    override fun availableBuffs(): Int {
        return bm.available()
    }

    fun lockShared(blk: BlockId, offset: Int) {
        TODO("not implemented")
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
