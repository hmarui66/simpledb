package simpledb.tx.concurrency.rowlock

import simpledb.record.RID
import simpledb.tx.rowlock.TransactionImpl
import java.util.concurrent.locks.ReentrantLock

class LockTable {
    private val lock = ReentrantLock()
    private val exclusiveLockTable: MutableMap<RID, Int> = HashMap()
    private val sharedLockTable: MutableMap<RID, MutableList<Int>> = HashMap()
    fun lockShared(tx: TransactionImpl, rid: RID): Boolean {
        lock.lock()
        try {
            val txNum = exclusiveLockTable[rid]
            if (txNum != null) {
                return txNum == tx.txNum
            }
            val txList = sharedLockTable.getOrDefault(rid, mutableListOf())
            if (txList.contains(tx.txNum)) {
                return true
            }
            txList.add(tx.txNum)
            sharedLockTable[rid] = txList
            tx.addSharedLockRID(rid)
            return true
        } finally {
            lock.unlock()
        }
    }

    fun lockExclusive(tx: TransactionImpl, rid: RID): Boolean {
        lock.lock()
        try {
            val txNum = exclusiveLockTable[rid]
            if (txNum != null) {
                return txNum == tx.txNum
            }
            val txList = sharedLockTable[rid]
            if (txList != null) {
                if (!(txList.size == 1 && txList.first() == tx.txNum)) {
                    return false
                }
            }

            exclusiveLockTable[rid] = tx.txNum
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
                if (exclusiveLockTable[it] == tx.txNum) {
                    exclusiveLockTable.remove(it)
                }
                sharedLockTable[it]?.remove(tx.txNum)
            }
        } finally {
            lock.unlock()
        }

    }
}
