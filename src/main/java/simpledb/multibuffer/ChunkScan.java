package simpledb.multibuffer;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;

public class ChunkScan implements Scan {
    private List<RecordPage> buffs = new ArrayList<>();
    private Transaction tx;
    private String fileName;
    private Layout layout;
    private int startBNum, endBNum, currentBNum;
    private RecordPage rp;
    private int currentSlot;

    /**
     * Create a chunk consisting of the specified pages.
     */
    public ChunkScan(Transaction tx, String fileName, Layout layout, int startBNum, int endBNum) {
        this.tx = tx;
        this.fileName = fileName;
        this.layout = layout;
        this.startBNum = startBNum;
        this.endBNum = endBNum;
        for (int i = startBNum; i<= endBNum; i++) {
            BlockId blk= new BlockId(fileName, i);
            buffs.add(new RecordPage(tx, blk, layout));
        }
        moveToBlock(startBNum);
    }

    @Override
    public void close() {
        for (int i = 0; i < buffs.size(); i++) {
            BlockId blk = new BlockId(fileName, startBNum+i);
            tx.unpin(blk);
        }
    }

    @Override
    public void beforeFirst() {
        moveToBlock(startBNum);
    }

    /**
     * Moves to the next record in the current block of the chunk.
     * If there are no more records, then make
     * the next block be current.
     * If there are no more blocks in the chunk, return false.
     */
    @Override
    public boolean next() {
        currentSlot = rp.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (currentBNum == endBNum) {
                return false;
            }
            moveToBlock(rp.block().number()+1);
            currentSlot = rp.nextAfter(currentSlot);
        }
        return true;
    }

    @Override
    public int getInt(String fieldName) {
        return rp.getInt(currentSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return rp.getString(currentSlot, fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (layout.schema().type(fieldName) == INTEGER) {
            return new Constant(getInt(fieldName));
        } else {
            return new Constant(getString(fieldName));
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.schema().hasField(fieldName);
    }

    private void moveToBlock(int blkNum) {
        currentBNum = blkNum;
        rp = buffs.get(currentBNum - startBNum);
        currentSlot = -1;
    }
}
