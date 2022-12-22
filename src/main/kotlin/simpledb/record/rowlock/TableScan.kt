package simpledb.record.rowlock

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.query.UpdateScan
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.tx.Transaction
import java.sql.Types

class TableScan(
    private val tx: Transaction,
    tableName: String,
    private val layout: Layout
) : UpdateScan {
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

    override fun close() {
        if (rp != null) {
            tx.unpin(rp!!.block())
        }
    }

    override fun getVal(fieldName: String): Constant {
        return if (layout.schema()
            .type(fieldName) == Types.INTEGER
        ) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
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

    override fun getInt(fldname: String): Int {
        return rp!!.getInt(currentSlot, fldname)
    }

    override fun getString(fldname: String): String {
        return rp!!.getString(currentSlot, fldname)
    }

    override fun hasField(fldname: String): Boolean {
        return layout.schema().hasField(fldname)
    }

    override fun setVal(fieldName: String, value: Constant) {
        if (layout.schema().type(fieldName) == Types.INTEGER) {
            setInt(fieldName, value.asInt())
        } else {
            setString(
                fieldName,
                value.asString()
            )
        }
    }

    override fun setInt(fldname: String, value: Int) {
        rp!!.setInt(currentSlot, fldname, value)
    }

    override fun setString(fldname: String, value: String) {
        rp!!.setString(currentSlot, fldname, value)
    }

    override fun insert() {
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

    override fun delete() {
        rp!!.delete(currentSlot)
    }

    override fun moveToRid(rid: RID) {
        close()
        val blk = BlockId(filename, rid.blockNumber())
        rp = RecordPage(tx, blk, layout)
        currentSlot = rid.slot()
    }

    override fun getRid(): RID {
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
