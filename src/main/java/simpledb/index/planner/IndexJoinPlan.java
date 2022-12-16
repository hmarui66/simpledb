package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.index.query.IndexJoinScan;
import simpledb.metadata.IndexInfo;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.record.TableScan;

public class IndexJoinPlan implements Plan {
    private Plan p1, p2;
    private IndexInfo ii;
    private String joinField;
    private Schema sch = new Schema();

    public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinField) {
        this.p1 = p1;
        this.p2 = p2;
        this.ii = ii;
        this.joinField = joinField;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    /**
     * Opens an indexJoin scan for this query
     */
    public Scan open() {
        Scan s = p1.open();
        TableScan ts = (TableScan) p2.open();
        Index idx = ii.open();
        return new IndexJoinScan(s, idx, joinField, ts);
    }

    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * <pre> B(indexJoin(p1,p2,idx) = B(p1) + R(p1)*B(idx)
     *       + R(indexJoin(p1,p2,idx) </pre>
     */
    public int blockAccessed() {
        return p1.blockAccessed()
                + (p1.recordsOutput() * ii.blocksAccessed())
                + recordsOutput();
    }

    /**
     * Estimates the number of output records in the join.
     * The formula is:
     * <pre> R(indexJoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
     */
    public int recordsOutput() {
        return p1.recordsOutput() * ii.recordsOutput();
    }

    /**
     * Estimates the number of distinct values for the
     * specified field.
     */
    public int distinctValues(String fieldName) {
        if (p1.schema().hasField(fieldName))
            return p1.distinctValues(fieldName);
        else
            return p2.distinctValues(fieldName);
    }

    /**
     * Returns the schema of the index join.
     */
    public Schema schema() {
        return sch;
    }
}
