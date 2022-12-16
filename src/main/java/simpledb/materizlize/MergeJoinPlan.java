package simpledb.materizlize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Arrays;
import java.util.List;

public class MergeJoinPlan implements Plan {
    private Plan p1, p2;
    private String fieldName1, fieldName2;
    private Schema sch = new Schema();

    /**
     * Creates a mergeJoin plan for the two specified queries.
     * The RHS must be materialized after it is sorted,
     * in order to deal with possible duplicates..
     */
    public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fieldName1, String fieldName2) {
        this.fieldName1 = fieldName1;
        List<String> sortList1 = Arrays.asList(fieldName1);
        this.p1 = new SortPlan(tx, p1, sortList1);

        this.fieldName2 = fieldName2;
        List<String> sortList2 = Arrays.asList(fieldName2);
        this.p2 = new SortPlan(tx, p2, sortList2);

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        SortScan s2 = (SortScan) p2.open();
        return new MergeJoinScan(s1, s2, fieldName1, fieldName2);
    }

    /**
     * Return the number of block accesses required to
     * mergeJoin the sorted tables.
     * Since a mergeJoin can be preformed with a single
     * pass through each table, the method returns
     * the sum of the block accesses of the
     * materialized sorted tables.
     * It does not include the onetime cost
     * of materializing and sorting the records.
     */
    @Override
    public int blockAccessed() {
        return p1.blockAccessed() + p2.blockAccessed();
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * <pre> R(join(p1, p2)) = R(p1)*R(p2)/max{V(p1, F1), V(p2, F2)}</pre>
     */
    @Override
    public int recordsOutput() {
        int maxVals = Math.max(p1.distinctValues(fieldName1),
                p2.distinctValues(fieldName2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxVals;
    }

    @Override
    public int distinctValues(String fieldName) {
        if (p1.schema().hasField(fieldName)) {
            return p1.distinctValues(fieldName);
        } else {
            return p2.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
