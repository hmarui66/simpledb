package simpledb.tx.concurrency.rowlock

import simpledb.record.RID
import simpledb.tx.rowlock.TransactionImpl
import java.util.concurrent.locks.ReentrantLock

class LockTable {
    private val lock = ReentrantLock()
    private val mutExclusiveLockTable: MutableMap<RID, Int> = HashMap()
    private val mutSharedLockTable: MutableMap<RID, MutableList<Int>> = HashMap()

    val exclusiveLockTable: Map<RID, Int>
        get() = mutExclusiveLockTable.toMap()

    val sharedLockTable: Map<RID, List<Int>>
        get() = mutSharedLockTable.toMap()

    fun lockShared(tx: TransactionImpl, rid: RID): Boolean {
        lock.lock()
        try {
            val txNum = mutExclusiveLockTable[rid]
            if (txNum != null) {
                return txNum == tx.txNum
            }
            val txList = mutSharedLockTable.getOrDefault(rid, mutableListOf())
            if (txList.contains(tx.txNum)) {
                return true
            }
            txList.add(tx.txNum)
            mutSharedLockTable[rid] = txList
            tx.addSharedLockRID(rid)
            return true
        } finally {
            lock.unlock()
        }
    }

    fun lockExclusive(tx: TransactionImpl, rid: RID): Boolean {
        lock.lock()
        try {
            val txNum = mutExclusiveLockTable[rid]
            if (txNum != null) {
                return txNum == tx.txNum
            }
            val txList = mutSharedLockTable[rid]
            if (txList != null && txList.size > 0) {
                if (!(txList.size == 1 && txList.first() == tx.txNum)) {
                    return false
                }
            }

            mutExclusiveLockTable[rid] = tx.txNum
            tx.addExclusiveLockRID(rid)
            return true
        } finally {
            lock.unlock()
        }
    }

    fun unlock(tx: TransactionImpl, ridList: List<RID>) {
        lock.lock()
        try {
            ridList.forEach {
                if (mutExclusiveLockTable[it] == tx.txNum) {
                    mutExclusiveLockTable.remove(it)
                }
                mutSharedLockTable[it]?.remove(tx.txNum)
            }
        } finally {
            lock.unlock()
        }

    }
}
