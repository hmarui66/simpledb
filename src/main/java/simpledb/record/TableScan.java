package simpledb.record;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class TableScan implements UpdateScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage rp;
    private String filename;
    private int currentslot;

    public TableScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
        if (tx.size(filename) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (rp != null) {
            tx.unpin(rp.block());
        }
    }

    @Override
    public Constant getVal(String fieldName) {
        if (layout.schema().type(fieldName) == INTEGER)
            return new Constant(getInt(fieldName));
        else
            return new Constant(getString(fieldName));
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentslot = rp.nextAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(rp.block().number() + 1);
            currentslot = rp.nextAfter(currentslot);
        }
        return true;
    }

    public int getInt(String fldname) {
        return rp.getInt(currentslot, fldname);
    }

    public String getString(String fldname) {
        return rp.getString(currentslot, fldname);
    }

    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    @Override
    public void setVal(String fieldName, Constant val) {
        if (layout.schema().type(fieldName) == INTEGER)
            setInt(fieldName, val.asInt());
        else
            setString(fieldName, val.asString());
    }

    public void setInt(String fldname, int val) {
        rp.setInt(currentslot, fldname, val);
    }

    public void setString(String fldname, String val) {
        rp.setString(currentslot, fldname, val);
    }

    public void insert() {
        currentslot = rp.insertAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(rp.block().number() + 1);
            }
            currentslot = rp.insertAfter(currentslot);
        }
    }

    public void delete() {
        rp.delete(currentslot);
    }

    public void moveToRid(RID rid) {
        close();
        BlockId blk = new BlockId(filename, rid.blockNumber());
        rp = new RecordPage(tx, blk, layout);
        currentslot = rid.slot();
    }

    public RID getRid() {
        return new RID(rp.block().number(), currentslot);
    }

    private void moveToBlock(int blknum) {
        close();
        BlockId blk = new BlockId(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(filename);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentslot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().number() == tx.size(filename) - 1;
    }
}
