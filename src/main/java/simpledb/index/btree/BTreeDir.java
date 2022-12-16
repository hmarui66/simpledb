package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class BTreeDir {
    private Transaction tx;
    private Layout layout;
    private BTPage contents;
    private String fileName;

    /**
     * Creates an object to hold the contents of the specified
     * B-tree block.
     */
    BTreeDir(Transaction tx, BlockId blk, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        contents = new BTPage(tx, blk, layout);
        fileName = blk.fileName();
    }

    /**
     * Closes the directory page.
     */
    public void close() {
        contents.close();
    }

    /**
     * Returns the block number of the B-tree leaf block
     * that contains the specified search key.
     */
    public int search(Constant searchKey) {
        System.out.println("start search by " + searchKey);
        BlockId childBlk = findChildBlock(searchKey);
        while (contents.getFlag() > 0) {
            contents.close();
            contents = new BTPage(tx, childBlk, layout);
            childBlk = findChildBlock(searchKey);
        }
        return childBlk.number();
    }

    /**
     * Creates a new root block for the B-tree.
     * The new root will have two children:
     * the old root, and the specified block.
     * Since the root must always be in block 0 of the file,
     * the contents of the old root will get transferred to a new block.
     */
    public void makeNewRoot(DirEntry e) {
        Constant firstVal = contents.getDataVal(0);
        int level = contents.getFlag();
        BlockId newBlk = contents.split(0, level);
        DirEntry oldRoot = new DirEntry(firstVal, newBlk.number());
        insertEntry(oldRoot);
        insertEntry(e);
        contents.setFlag(level + 1);
    }

    /**
     * Inserts a new directory entry into the B-tree block.
     * If the block is at level 0, then the entry is inserted there.
     * Otherwise, the entry is inserted into the appropriate
     * child node, and the return value is examined.
     * A non-null return value indicates that the child node
     * split, and so the returned entry is inserted into
     * this block.
     * If this block splits, then the method similarly returns
     * the entry information of the new block to its caller;
     * otherwise, the method returns null.
     */
    public DirEntry insert(DirEntry e) {
        if (contents.getFlag() == 0)
            return insertEntry(e);
        BlockId childBlk = findChildBlock(e.dataVal());
        BTreeDir child = new BTreeDir(tx, childBlk, layout);
        DirEntry myEntry = child.insert(e);
        child.close();
        return (myEntry != null) ? insertEntry(myEntry) : null;
    }

    private DirEntry insertEntry(DirEntry e) {
        int newSlot = 1 + contents.findSlotBefore(e.dataVal());
        contents.insertDir(newSlot, e.dataVal(), e.blockNumber());
        if (!contents.isFull())
            return null;
        // ページが full の場合 split する
        int level = contents.getFlag();
        int splitPos = contents.getNumRecs() / 2;
        Constant splitVal = contents.getDataVal(splitPos);
        BlockId newBlk = contents.split(splitPos, level);
        return new DirEntry(splitVal, newBlk.number());
    }

    private BlockId findChildBlock(Constant searchKey) {
        int slot = contents.findSlotBefore(searchKey);
        if (contents.getDataVal(slot+1).equals(searchKey))
            slot++;
        int blkNum = contents.getChildNum(slot);
        return new BlockId(fileName, blkNum);
    }
}
