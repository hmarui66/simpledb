package simpledb.materizlize;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.List;

public class GroupByPlan implements Plan {
    private Plan p;
    private List<String> groupFields;
    private List<AggregateFn> aggFns;
    private Schema schema = new Schema();

    /**
     * Create a groupBy plan for the underlying query.
     * The grouping is determined by the specified
     * collection of group fields,
     * and the aggregation is computed by the
     * specified collection of aggregation functions.
     */
    public GroupByPlan(Transaction tx, Plan p, List<String> groupFields, List<AggregateFn> aggFns) {
        this.p = new SortPlan(tx, p, groupFields);
        this.groupFields = groupFields;
        this.aggFns = aggFns;
        for (String fieldName : groupFields)
            schema.add(fieldName, p.schema());
        for (AggregateFn fn: aggFns) {
            schema.addIntField(fn.fieldName());
        }
    }

    public GroupByPlan(Scan s, List<String> groupFields, List<AggregateFn> aggFns) {
    }

    /**
     * This method opens a sort plan for the specified plan.
     * The sort plan ensures that the underlying records
     * will be appropriately grouped.
     */
    public Scan open() {
        Scan s = p.open();
        return new GroupByScan(s, groupFields, aggFns);
    }

    /**
     * Returns the number of blocks required to
     * compute the aggregation,
     * which is one pass through the sorted table.
     * It does not include the onetime cost
     * of materializing and sorting the records
     */
    @Override
    public int blockAccessed() {
        return p.blockAccessed();
    }

    /**
     * Returns the number of groups.
     * Assuming equal distribution,
     * this is the product of the distinct values
     * for each grouping field.
     */
    @Override
    public int recordsOutput() {
        int numGroups = 1;
        for (String fieldName : groupFields)
            numGroups *= p.distinctValues(fieldName);
        return numGroups;
    }

    /**
     * Returns the number of distinct values for the
     * specified field. If the field is a grouping field,
     * then the number of distinct values is the same
     * as in the underlying query.
     * If the field is an aggregate field, then we
     * assume that all values are distinct.
     */
    @Override
    public int distinctValues(String fieldName) {
        if (p.schema().hasField(fieldName))
            return p.distinctValues(fieldName);
        else
            return recordsOutput();
    }

    /**
     * Returns the schema of the output table.
     * The schema consists of the group fields,
     * plus one field for each aggregation function.
     */
    @Override
    public Schema schema() {
        return schema;
    }
}
