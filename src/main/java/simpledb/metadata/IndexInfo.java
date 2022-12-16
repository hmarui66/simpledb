package simpledb.metadata;

import simpledb.index.Index;
import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class IndexInfo {
    private String idxName, fieldName;
    private Transaction tx;
    private Schema tblSchema;
    private Layout idxLayout;
    private StatInfo si;

    public IndexInfo(String idxName, String fieldName, Schema tblSchema, Transaction tx, StatInfo si) {
        this.idxName = idxName;
        this.fieldName = fieldName;
        this.tx = tx;
        this.tblSchema = tblSchema;
        this.idxLayout = createIdxLayout();
        this.si = si;
    }

    public Index open() {
//        return new HashIndex(tx, idxName, idxLayout);
        return new BTreeIndex(tx, idxName, idxLayout);
    }

    public int blocksAccessed() {
        int rpb = tx.blockSize() / idxLayout.slotSize();
        int numBlocks = si.recordsOutput() / rpb;
        return HashIndex.searchCost(numBlocks, rpb);
    }

    public int recordsOutput() {
        return si.recordsOutput() / si.distinctValues(fieldName);
    }

    public int distinctValues(String fname) {
        return fieldName.equals(fname) ? 1 : si.distinctValues(fieldName);
    }

    private Layout createIdxLayout() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (tblSchema.type(fieldName) == INTEGER)
            sch.addIntField("dataVal");
        else {
            int fieldLen = tblSchema.length(fieldName);
            sch.addStringField("dataVal", fieldLen);
        }
        return new Layout(sch);
    }
}
