package simpledb.materizlize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MaterializePlan implements Plan {
    private Plan srcPlan;
    private Transaction tx;

    /**
     * Create a materialize plan for the specified query.
     */
    public MaterializePlan(Transaction tx, Plan srcPlan) {
        this.srcPlan = srcPlan;
        this.tx = tx;
    }

    /**
     * Loops through the underlying query,
     * copying its output records into a temporary table.
     * It then returns a table scan for that table.
     */
    @Override
    public Scan open() {
        Schema sch = srcPlan.schema();
        TempTable temp = new TempTable(tx, sch);
        Scan src = srcPlan.open();
        UpdateScan dest = temp.open();
        while (src.next()) {
            dest.insert();
            for (String fieldName: sch.fields()) {
                dest.setVal(fieldName, src.getVal(fieldName));
            }
        }
        src.close();
        dest.beforeFirst();
        return dest;
    }

    /**
     * Return the estimated number of blocks in the
     * materialized table.
     * It does not include the one-time cost
     * of materializing the records.
     */
    @Override
    public int blockAccessed() {
        // レコードの長さを計算するためにダミーレイアウトを作成
        Layout layout = new Layout(srcPlan.schema());
        double rpb = (double) (tx.blockSize() / layout.slotSize());
        return (int) Math.ceil(srcPlan.recordsOutput() / rpb);
    }

    /**
     * Return the number of records in the materialized table,
     * which is the same as in the underlying plan.
     */
    @Override
    public int recordsOutput() {
        return srcPlan.recordsOutput();
    }

    /**
     * Return the number of distinct field values,
     * which is the same as in the underlying plan.
     */
    @Override
    public int distinctValues(String fieldName) {
        return srcPlan.distinctValues(fieldName);
    }

    /**
     * Return the schema of the materialized table,
     * which is the same as in the underlying plan.
     */
    @Override
    public Schema schema() {
        return srcPlan.schema();
    }
}
