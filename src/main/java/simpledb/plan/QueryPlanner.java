package simpledb.plan;

import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

public interface QueryPlanner {
    /**
     * Create a plan for the parsed query.
     */
    public Plan createPlan(QueryData data, Transaction tx);
}
