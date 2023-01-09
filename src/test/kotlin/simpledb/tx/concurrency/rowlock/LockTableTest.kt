package simpledb.tx.concurrency.rowlock

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.hasItem
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.hasSize
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.not
import org.junit.jupiter.api.Test
import simpledb.record.RID
import simpledb.server.SimpleDB
import simpledb.tx.rowlock.TransactionImpl

class LockTableTest {
    private val db = SimpleDB("dbdir/lockTableTest", 400, 8)

    @Test
    fun lockSharedSucceed() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(tx.sharedLockRIDs, hasSize(1))
        assertThat(tx.sharedLockRIDs.first(), `is`(rid))
        assertThat(lockTable.sharedLockTable, hasKey(rid))
        assertThat(lockTable.sharedLockTable[rid], contains(tx.txNum))
    }

    @Test
    fun lockSharedSucceedTwice() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(tx.sharedLockRIDs, hasSize(1))
        assertThat(tx.sharedLockRIDs.first(), `is`(rid))
        assertThat(lockTable.sharedLockTable, hasKey(rid))
        assertThat(lockTable.sharedLockTable[rid], contains(tx.txNum))
    }

    @Test
    fun lockSharedSucceedWhenSLockAcquiredByOtherTx() {
        val lockTable = LockTable()
        val rid = RID(0, 0)

        val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockShared(otherTx, rid)

        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(tx.sharedLockRIDs, hasSize(1))
        assertThat(tx.sharedLockRIDs.first(), `is`(rid))
        assertThat(lockTable.sharedLockTable, hasKey(rid))
        assertThat(lockTable.sharedLockTable[rid], hasItem(tx.txNum))
    }

    @Test
    fun lockExclusiveSucceed() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

        assertThat(lockTable.lockExclusive(tx, rid), `is`(true))
        assertThat(tx.exclusiveLockRIDs, hasSize(1))
        assertThat(tx.exclusiveLockRIDs.first(), `is`(rid))
        assertThat(lockTable.exclusiveLockTable, hasKey(rid))
        assertThat(lockTable.exclusiveLockTable[rid], `is`(tx.txNum))
    }

    @Test
    fun lockExclusiveSucceedTwice() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockExclusive(tx, rid)

        assertThat(lockTable.lockExclusive(tx, rid), `is`(true))
        assertThat(tx.exclusiveLockRIDs, hasSize(1))
        assertThat(tx.exclusiveLockRIDs.first(), `is`(rid))
        assertThat(lockTable.exclusiveLockTable, hasKey(rid))
        assertThat(lockTable.exclusiveLockTable[rid], `is`(tx.txNum))
    }

    @Test
    fun lockSharedSucceedButRidNotAppendedWhenXLockAcquired() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockExclusive(tx, rid)

        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(tx.sharedLockRIDs, hasSize(0))
        assertThat(lockTable.sharedLockTable, not(hasKey(rid)))
    }

    @Test
    fun lockExclusiveSucceedWhenSLockAcquired() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockShared(tx, rid)

        assertThat(lockTable.lockExclusive(tx, rid), `is`(true))
        assertThat(tx.exclusiveLockRIDs, hasSize(1))
        assertThat(tx.exclusiveLockRIDs.first(), `is`(rid))
        assertThat(lockTable.exclusiveLockTable, hasKey(rid))
        assertThat(lockTable.exclusiveLockTable[rid], `is`(tx.txNum))
    }

    @Test
    fun lockSharedFailWhenXLockAcquiredByOtherTx() {
        val lockTable = LockTable()
        val rid = RID(0, 0)

        val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockExclusive(otherTx, rid)

        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockShared(tx, rid), `is`(false))
    }

    @Test
    fun lockExclusiveFailWhenSLockAcquiredByOtherTx() {
        val lockTable = LockTable()
        val rid = RID(0, 0)

        val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockShared(otherTx, rid)

        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockExclusive(tx, rid), `is`(false))
    }

    @Test
    fun unlock() {
        val lockTable = LockTable()
        val rid = RID(0, 0)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockShared(tx, rid), `is`(true))

        lockTable.unlock(tx, listOf(rid))
        assertThat(lockTable.sharedLockTable[rid], not(hasItem(tx.txNum)))
    }

    @Test
    fun lockExclusiveSucceedWhenSLockAcquiredAndUnlockByOtherTx() {
        val lockTable = LockTable()
        val rid = RID(0, 0)

        val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockShared(otherTx, rid)
        lockTable.unlock(otherTx, listOf(rid))

        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockExclusive(tx, rid), `is`(true))
        assertThat(tx.exclusiveLockRIDs, hasSize(1))
        assertThat(tx.exclusiveLockRIDs.first(), `is`(rid))
        assertThat(lockTable.exclusiveLockTable, hasKey(rid))
        assertThat(lockTable.exclusiveLockTable[rid], `is`(tx.txNum))
    }

    @Test
    fun lockSharedSucceedWhenXLockAcquiredAndUnlockByOtherTx() {
        val lockTable = LockTable()
        val rid = RID(0, 0)

        val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        lockTable.lockExclusive(otherTx, rid)
        lockTable.unlock(otherTx, listOf(rid))

        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
        assertThat(lockTable.lockShared(tx, rid), `is`(true))
        assertThat(tx.sharedLockRIDs, hasSize(1))
        assertThat(tx.sharedLockRIDs.first(), `is`(rid))
        assertThat(lockTable.sharedLockTable, hasKey(rid))
        assertThat(lockTable.sharedLockTable[rid], hasItem(tx.txNum))
    }
}
