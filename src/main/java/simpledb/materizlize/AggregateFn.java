package simpledb.materizlize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public interface AggregateFn {
    /**
     * Use the current record of the specified scan
     * to be the first record in the group.
     */
    void processFirst(Scan s);

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     */
    void processNext(Scan s);

    /**
     * Returns the name of the new aggregation field.
     */
    String fieldName();

    /**
     * Returns the computed aggregation value.
     */
    Constant value();
}
