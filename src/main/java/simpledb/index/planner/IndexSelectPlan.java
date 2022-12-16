package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.index.query.IndexSelectScan;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class IndexSelectPlan implements Plan {
    private Plan p;
    private IndexInfo ii;
    private Constant val;

    public IndexSelectPlan(Plan p, IndexInfo ii, Constant val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }

    /**
     * Creates a new indexSelect scan for this query
     */
    public Scan open() {
        TableScan ts = (TableScan) p.open();
        Index idx = ii.open();
        return new IndexSelectScan(ts, idx, val);
    }

    /**
     * Estimates the number of block accessed to compute the
     * index selection, which is the same as the
     * index traversal cost plus the number of matching data records.
     */
    public int blockAccessed() {
        return ii.blocksAccessed() + recordsOutput();
    }

    /**
     * Estimates the number of output records in the index selection,
     * which is the same as the number of search key values
     * for the index.
     */
    public int recordsOutput() {
        return ii.recordsOutput();
    }

    /**
     * Returns the distinct values as defined by the index.
     */
    public int distinctValues(String fieldName) {
        return ii.distinctValues(fieldName);
    }

    public Schema schema() {
        return p.schema();
    }
}
