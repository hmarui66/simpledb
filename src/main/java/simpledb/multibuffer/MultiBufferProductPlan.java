package simpledb.multibuffer;

import simpledb.materizlize.MaterializePlan;
import simpledb.materizlize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class MultiBufferProductPlan implements Plan {
    private Transaction tx;
    private Plan lhs, rhs;
    private Schema scheme = new Schema();

    public MultiBufferProductPlan(Transaction tx, Plan lhs, Plan rhs) {
        this.tx = tx;
        this.lhs = lhs;
        this.rhs = rhs;
        scheme.addAll(lhs.schema());
        scheme.addAll(rhs.schema());
    }

    /**
     * A scan for this query is created and returned, as follows.
     * First, the method materializes its LHS and RHS queries.
     * It then determines the optimal chunk size,
     * based on the size of the materialized RHS file and the
     * number of available buffers.
     * It creates a chunk plan for each chunk, saving them in a list.
     * Finally, it creates a multiScan for this list of plans,
     * and returns that scan
     */
    public Scan open() {
        Scan leftScan = lhs.open();
        TempTable tt = copyRecordsFrom(rhs);
        return new MultiBufferProductScan(tx, leftScan, tt.tableName(), tt.getLayout());
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * <pre> B(product(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
     * where C(p2) is the number of chunks of p2.
     * The method uses the current number of available buffers
     * to calculate C(p2), and so this value may differ
     * when the query scan is opened.
     */
    @Override
    public int blockAccessed() {
        // this guesses at the # of chunks
        int avail = this.tx.availableBuffs();
        int size = new MaterializePlan(tx, rhs).blockAccessed();
        int numChunks = size / avail;
        return rhs.blockAccessed() +
                (lhs.blockAccessed() * numChunks);
    }

    /**
     * Estimates the number of output records in the product.
     * The formula is:
     * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
     */
    @Override
    public int recordsOutput() {
        return lhs.recordsOutput() * rhs.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (lhs.schema().hasField(fieldName)) {
            return lhs.distinctValues(fieldName);
        } else {
            return rhs.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return scheme;
    }

    private TempTable copyRecordsFrom(Plan p) {
        Scan src = p.open();
        Schema sch = p.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = (UpdateScan) t.open();
        while (src.next()) {
            dest.insert();
            for (String fieldName : sch.fields()) {
                dest.setVal(fieldName, src.getVal(fieldName));
            }
        }
        src.close();
        dest.close();
        return t;
    }
}
