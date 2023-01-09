package simpledb.record.rowlock

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.tx.rowlock.TransactionImpl
import java.sql.Types

class TableScan(
    private val tx: TransactionImpl,
    tableName: String,
    private val layout: Layout
)  {
    private var rp: RecordPage? = null
    private val filename: String
    private var currentSlot = 0

    init {
        filename = "$tableName.tbl"
        if (tx.size(filename) == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    fun close() {
        if (rp != null) {
            tx.unpin(rp!!.block())
        }
    }

    fun getVal(fieldName: String): Constant {
        return if (layout.schema()
            .type(fieldName) == Types.INTEGER
        ) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    fun beforeFirst() {
        moveToBlock(0)
    }

    fun next(): Boolean {
        currentSlot = rp!!.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false
            }
            moveToBlock(rp!!.block().number() + 1)
            currentSlot = rp!!.nextAfter(currentSlot)
        }
        return true
    }

    fun getInt(fldname: String): Int {
        return rp!!.getInt(currentSlot, fldname)
    }

    fun getString(fldname: String): String {
        return rp!!.getString(currentSlot, fldname)
    }

    fun hasField(fldname: String): Boolean {
        return layout.schema().hasField(fldname)
    }

    fun setVal(fieldName: String, value: Constant) {
        if (layout.schema().type(fieldName) == Types.INTEGER) {
            setInt(fieldName, value.asInt())
        } else {
            setString(
                fieldName,
                value.asString()
            )
        }
    }

    fun setInt(fldname: String, value: Int): Boolean =
        rp!!.setInt(currentSlot, fldname, value)

    fun setString(fldname: String, value: String): Boolean =
        rp!!.setString(currentSlot, fldname, value)

    fun insert() {
        currentSlot = rp!!.insertAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock()
            } else {
                moveToBlock(rp!!.block().number() + 1)
            }
            currentSlot = rp!!.insertAfter(currentSlot)
        }
    }

    fun delete() {
        rp!!.delete(currentSlot)
    }

    fun moveToRid(rid: RID) {
        close()
        val blk = BlockId(filename, rid.blockNumber())
        rp = RecordPage(tx, blk, layout)
        currentSlot = rid.slot()
    }

    fun getRid(): RID {
        return RID(rp!!.block().number(), currentSlot)
    }

    private fun moveToBlock(blkNum: Int) {
        close()
        val blk = BlockId(filename, blkNum)
        rp = RecordPage(tx, blk, layout)
        currentSlot = -1
    }

    private fun moveToNewBlock() {
        close()
        val blk = tx.append(filename)
        rp = RecordPage(tx, blk, layout)
        rp!!.format()
        currentSlot = -1
    }

    private fun atLastBlock(): Boolean {
        return rp!!.block().number() == tx.size(filename) - 1
    }
}
