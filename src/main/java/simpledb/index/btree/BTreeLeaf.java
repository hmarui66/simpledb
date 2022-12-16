package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.tx.Transaction;

public class BTreeLeaf {
    private Transaction tx;
    private Layout layout;
    private Constant searchKey;
    private BTPage contents;
    private int currentSlot;
    private String fileName;

    /**
     * Opens a buffer to hold the specified leaf block.
     * The buffer is positioned immediately before the first record
     * having the specified search key (if any).
     */
    public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, Constant searchKey) {
        this.tx = tx;
        this.layout = layout;
        this.searchKey = searchKey;
        contents = new BTPage(tx, blk, layout);
        currentSlot = contents.findSlotBefore(searchKey);
        fileName = blk.fileName();
    }

    /**
     * Close the leaf page
     */
    public void close() {
        contents.close();
    }

    /**
     * Moves to the next leaf record having the
     * previously-specified search key.
     * Returns false if there is no more such records.
     */
    public boolean next() {
        currentSlot++;
        if (currentSlot >= contents.getNumRecs())
            return tryOverflow();
        else if (contents.getDataVal(currentSlot).equals(searchKey))
            return true;
        else
            return tryOverflow();
    }

    /**
     * Returns the dataRID value of the current leaf record.
     */
    public RID getDataRid() {
        return contents.getDataRid(currentSlot);
    }

    /**
     * Deletes the leaf record having the specified dataRID
     */
    public void delete(RID dataRid) {
        while (next())
            if (getDataRid().equals(dataRid)) {
                contents.delete(currentSlot);
                return;
            }
    }

    /**
     * Inserts a new leaf record having the specified dataRID
     * and the previously-specified search key.
     * If the record does no fit in the page, then
     * the page splits and the method returns the
     * directory entry for the new page;
     * otherwise, the method returns null.
     * If all the records in the page have the same dataVal,
     * then the block does not split; instead, all but one of the
     * records are placed into an overflow block.
     */
    public DirEntry insert(RID dataRid) {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            Constant firstVal = contents.getDataVal(0);
            BlockId newBlk = contents.split(0, contents.getFlag());
            currentSlot = 0;
            contents.setFlag(-1);
            contents.insertLeaf(currentSlot, searchKey, dataRid);
            return new DirEntry(firstVal, newBlk.number());
        }

        currentSlot++;
        contents.insertLeaf(currentSlot, searchKey, dataRid);
        if (!contents.isFull())
            return null;
        // ページが full の場合、split する
        Constant firstKey = contents.getDataVal(0);
        Constant lastKey = contents.getDataVal(contents.getNumRecs()-1);
        if (lastKey.equals(firstKey)) {
            // overflow ブロックを生成して、2件目以降のレコードを移す
            BlockId newBlk = contents.split(1, contents.getFlag());
            contents.setFlag(newBlk.number());
            return null;
        } else {
            int splitPos = contents.getNumRecs() / 2;
            Constant splitKey = contents.getDataVal(splitPos);
            if (splitKey.equals(firstKey)) {
                // next key を探すために右へ移動
                while (contents.getDataVal(splitPos).equals(splitKey))
                    splitPos++;
                splitKey = contents.getDataVal(splitPos);
            } else {
                // key を持つ最初の entry を探すために左へ移動
                while (contents.getDataVal(splitPos-1).equals(splitKey))
                    splitPos--;
            }
            BlockId newBlk = contents.split(splitPos, -1);
            return new DirEntry(splitKey, newBlk.number());
        }
    }

    private boolean tryOverflow() {
        Constant firstKey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0)
            return false;
        contents.close();
        BlockId nextBlk = new BlockId(fileName, flag);
        contents = new BTPage(tx, nextBlk, layout);
        currentSlot = 0;
        return true;
    }
}
