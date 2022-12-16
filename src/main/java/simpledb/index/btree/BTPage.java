package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class BTPage {
    private Transaction tx;
    private BlockId currentBlk;
    private Layout layout;

    public BTPage(Transaction tx, BlockId currentBlk, Layout layout) {
        this.tx = tx;
        this.currentBlk = currentBlk;
        this.layout = layout;
        tx.pin(currentBlk);
    }

    /**
     * Calculate the position where the first record having
     * the specified search key should be, then returns
     * the position before it
     */
    public int findSlotBefore(Constant searchKey) {
        int slot = 0;
        while (slot < getNumRecs() && getDataVal(slot).compareTo(searchKey) < 0)
            slot++;
        return slot - 1;
    }

    /**
     * Close the page by unpinning its buffer
     */
    public void close() {
        if (currentBlk != null)
            tx.unpin(currentBlk);
        currentBlk = null;
    }

    /**
     * Return true if the block is full.
     */
    public boolean isFull() {
        return slotPos(getNumRecs() + 1) >= tx.blockSize();
    }

    /**
     * Split the page at the specified position.
     * A new page is created, and the records of the page
     * starting at the split position are transferred to the new page.
     */
    public BlockId split(int splitPos, int flag) {
        BlockId newBlk = appendNew(flag);
        BTPage newPage = new BTPage(tx, newBlk, layout);
        transferRecs(splitPos, newPage);
        newPage.setFlag(flag);
        newPage.close();
        return newBlk;
    }

    /**
     * Return the dataVal of the record at the specified slot.
     */
    public Constant getDataVal(int slot) {
        return getVal(slot, "dataVal");
    }

    /**
     * Retuen the value of the page's flag field.
     */
    public int getFlag() {
        return tx.getInt(currentBlk, 0);
    }


    /**
     * Set the page's flag field to the specified value
     */
    public void setFlag(int val) {
        tx.setInt(currentBlk, 0, val, true);
    }

    /**
     * Append a new block to the end of the specified B-tree file,
     * having the specified flag value.
     */
    private BlockId appendNew(int flag) {
        BlockId blk = tx.append(currentBlk.fileName());
        tx.pin(blk);
        format(blk, flag);
        return blk;
    }

    public void format(BlockId blk, int flag) {
        tx.setInt(blk, 0, flag, false);
        tx.setInt(blk, Integer.BYTES, 0, false);
        int recSize = layout.slotSize();
        for (int pos = 2 * Integer.BYTES; pos + recSize <= tx.blockSize(); pos += recSize)
            makeDefaultRecord(blk, pos);
    }

    private void makeDefaultRecord(BlockId blk, int pos) {
        for (String fieldName : layout.schema().fields()) {
            int offset = layout.offset(fieldName);
            if (layout.schema().type(fieldName) == INTEGER)
                tx.setInt(blk, pos + offset, 0, false);
            else
                tx.setString(blk, pos + offset, "", false);
        }
    }

    // Methods called only by BTreeDir

    /**
     * Return the block number stored in the index record
     * at the specified slot.
     */
    public int getChildNum(int slot) {
        return getInt(slot, "block");
    }

    /**
     * Insert a directory entry at the specified slot.
     */
    public void insertDir(int slot, Constant val, int blkNum) {
        insert(slot);
        setVal(slot, "dataVal", val);
        setInt(slot, "block", blkNum);
    }

    /**
     * Return the dataRID value stored in the specified leaf index record.
     */
    public RID getDataRid(int slot) {
        return new RID(getInt(slot, "block"), getInt(slot, "id"));
    }

    /**
     * Insert a leaf index record at the specified slot.
     */
    public void insertLeaf(int slot, Constant val, RID rid) {
        insert(slot);
        setVal(slot, "dataVal", val);
        setInt(slot, "block", rid.blockNumber());
        setInt(slot, "id", rid.slot());
    }

    /**
     * Delete the index record at the specified slot.
     */
    public void delete(int slot) {
        for (int i = slot+1; i<getNumRecs(); i++)
            copyRecord(i, i-1);
        setNumRecs(getNumRecs()-1);
    }

    /**
     * Return the number of index records in this page.
     */
    public int getNumRecs() {
        return tx.getInt(currentBlk, Integer.BYTES);
    }

    // Private methods

    private int getInt(int slot, String fieldName) {
        int pos = fieldPos(slot, fieldName);
        return tx.getInt(currentBlk, pos);
    }

    private String getString(int slot, String fieldName) {
        int pos = fieldPos(slot, fieldName);
        return tx.getString(currentBlk, pos);
    }

    private Constant getVal(int slot, String fieldName) {
        int type = layout.schema().type(fieldName);
        if (type == INTEGER)
            return new Constant(getInt(slot, fieldName));
        else
            return new Constant(getString(slot, fieldName));
    }

    private void setInt(int slot, String fieldName, int val) {
        int pos = fieldPos(slot, fieldName);
        tx.setInt(currentBlk, pos, val, true);
    }

    private void setString(int slot, String fieldName, String val) {
        int pos = fieldPos(slot, fieldName);
        tx.setString(currentBlk, pos, val, true);
    }

    private void setVal(int slot, String fieldName, Constant val) {
        int type = layout.schema().type(fieldName);
        if (type == INTEGER)
            setInt(slot, fieldName, val.asInt());
        else
            setString(slot, fieldName, val.asString());
    }

    private void setNumRecs(int n) {
        tx.setInt(currentBlk, Integer.BYTES, n, true);
    }

    private void insert(int slot) {
        for (int i = getNumRecs(); i > slot ; i--)
            copyRecord(i-1, i);
        setNumRecs(getNumRecs()+1);
    }

    private void copyRecord(int from, int to) {
        Schema sch = layout.schema();
        for (String fieldName : sch.fields())
            setVal(to, fieldName, getVal(from, fieldName));
    }

    private void transferRecs(int slot, BTPage dest) {
        int destSlot = 0;
        while (slot < getNumRecs()) {
            dest.insert(destSlot);
            Schema sch = layout.schema();
            for (String fieldName : sch.fields())
                dest.setVal(destSlot, fieldName, getVal(slot, fieldName));
            delete(slot);
            destSlot++;
        }
    }

    private int fieldPos(int slot, String fieldName) {
        int offset = layout.offset(fieldName);
        return slotPos(slot) + offset;
    }

    private int slotPos(int slot) {
        int slotSize = layout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotSize);
    }

}
