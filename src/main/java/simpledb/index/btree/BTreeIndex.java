package simpledb.index.btree;

import simpledb.file.BlockId;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class BTreeIndex implements Index {
    private Transaction tx;
    private Layout dirLayout, leafLayout;
    private String leafTbl;
    private BTreeLeaf leaf = null;
    private BlockId rootBlk;

    /**
     * Opens a B-tree index for the specified index.
     * The method determines the appropriate files
     * for the leaf and directory records.
     * creating them if they did not exist.
     */
    public BTreeIndex(Transaction tx, String idxName, Layout leafLayout) {
        this.tx = tx;
        leafTbl = idxName + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leafTbl) == 0) {
            BlockId blk = tx.append(leafTbl);
            BTPage node = new BTPage(tx, blk, leafLayout);
            node.format(blk, -1);
        }

        Schema dirSch = new Schema();
        dirSch.add("block", leafLayout.schema());
        dirSch.add("dataVal", leafLayout.schema());
        String dirTbl = idxName + "dir";
        dirLayout = new Layout(dirSch);
        rootBlk = new BlockId(dirTbl, 0);
        if (tx.size(dirTbl) == 0) {
            // 新しいルートブロックを生成
            tx.append(dirTbl);
            BTPage node = new BTPage(tx, rootBlk, dirLayout);
            node.format(rootBlk, 0);
            // 最初のディレクトリエントリーを挿入
            int fieldType = dirSch.type("dataVal");
            Constant minVal = (fieldType == INTEGER) ? new Constant(Integer.MIN_VALUE) : new Constant("");
            node.insertDir(0, minVal, 0);
            node.close();
        }
    }

    /**
     * Traverse the directory to find the leaf block corresponding
     * to the specified search key.
     * The method then opens a page for that leaf block, and
     * positions the page before the first record (if any)
     * having that search key.
     * The leaf page is kept open, for use by the methods next
     * and getDataRid
     */
    public void beforeFirst(Constant searchKey) {
        close();
        BTreeDir root = new BTreeDir(tx, rootBlk, dirLayout);
        int blkNum = root.search(searchKey);
        root.close();
        BlockId leafBlk = new BlockId(leafTbl, blkNum);
        leaf = new BTreeLeaf(tx, leafBlk, leafLayout, searchKey);
    }

    /**
     * Move to the next leaf record having the
     * previously-specified search key.
     * Returns false if there are no more such leaf records.
     */
    public boolean next() {
        return leaf.next();
    }

    /**
     * Returns the dataRID value from the curren leaf record.
     */
    public RID getDataRid() {
        return leaf.getDataRid();
    }

    /**
     * Insert the specified record into the index.
     * The method first traverses the directory to find
     * the appropriate leaf page; then it inserts
     * the record into the leaf.
     * If the insertion causes the leaf to split, then
     * the method calls insert on the root,
     * passing it the directory entry of the new leaf page.
     * If the root node splits, then makeNewRoot is called.
     */
    public void insert(Constant dataVal, RID dataRid) {
        beforeFirst(dataVal);
        DirEntry e = leaf.insert(dataRid);
        leaf.close();
        if (e == null)
            return;
        BTreeDir root = new BTreeDir(tx, rootBlk, dirLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null)
            root.makeNewRoot(e2);
        root.close();
    }

    /**
     * Delete the specified index record.
     * The method first traverses the directory to find
     * the leaf page containing that record; then it
     * deletes the record from the page
     */
    public void delete(Constant dataVal, RID dataRid) {
        beforeFirst(dataVal);
        leaf.delete(dataRid);
        leaf.close();
    }

    /**
     * Close the index by closing its open leaf page,
     * if necessary.
     */
    public void close() {
        if (leaf != null)
            leaf.close();
    }

    /**
     * Estimate the number of block accesses
     * required to find all index records having
     * a particular search key.
     */
    public static int searchCost(int numBlocks, int rpb) {
        return 1 + (int)(Math.log(numBlocks) / Math.log(rpb));
    }

}
