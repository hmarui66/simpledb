package simpledb.record.rowlock

import simpledb.file.BlockId
import simpledb.record.Layout
import simpledb.tx.rowlock.TransactionImpl
import java.sql.Types

class RecordPage(private val tx: TransactionImpl, private val blk: BlockId, private val layout: Layout) {
    init {
        tx.pin(blk)
    }

    fun getInt(slot: Int, fldName: String): Int {
        val fldPos = offset(slot) + layout.offset(fldName)
        return tx.getInt(blk, fldPos)
    }

    fun getString(slot: Int, fldName: String): String {
        val fldPos = offset(slot) + layout.offset(fldName)
        return tx.getString(blk, fldPos)
    }

    fun setInt(slot: Int, fldName: String, value: Int) {
        tx.wLatchPage(blk)
        try {
            tx.lockExclusive(blk, slot)
            val fldPos = offset(slot) + layout.offset(fldName)
            tx.setInt(blk, fldPos, value, true)
        } finally {
            tx.wUnlatchPage(blk)
        }
    }

    fun setString(slot: Int, fldName: String, value: String) {
        tx.wLatchPage(blk)
        try {
            tx.lockExclusive(blk, slot)
            val fldPos = offset(slot) + layout.offset(fldName)
            tx.setString(blk, fldPos, value, true)
        } finally {
            tx.wUnlatchPage(blk)
        }
    }

    fun delete(slot: Int) {
        setFlag(slot, EMPTY)
    }

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY, false)
            val sch = layout.schema()
            for (fldName in sch.fields()) {
                val fldPos = offset(slot) + layout.offset(fldName)
                if (sch.type(fldName) == Types.INTEGER) {
                    tx.setInt(blk, fldPos, 0, false)
                } else {
                    tx.setString(blk, fldPos, "", false)
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, USED)
    }

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, EMPTY)
        if (newSlot >= 0) setFlag(newSlot, USED)
        return newSlot
    }

    fun block(): BlockId {
        return blk
    }

    private fun setFlag(slot: Int, flag: Int) {
        tx.setInt(blk, offset(slot), flag, true)
    }

    private fun searchAfter(orgSlot: Int, used: Int): Int {
        var slot = orgSlot
        slot++
        while (isValidSlot(slot)) {
            tx.rLatchPage(blk)
            try {
                tx.lockShared(blk, slot)
                if (tx.getInt(blk, offset(slot)) == used) {
                    return slot
                }
            } finally {
                tx.rUnlatchPage(blk)
            }
            slot++
        }
        return -1
    }

    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot + 1) <= tx.blockSize()
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }

    companion object {
        const val EMPTY = 0
        const val USED = 1
    }
}
