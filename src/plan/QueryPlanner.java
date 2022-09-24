package plan;

import parse.QueryData;
import tx.Transaction;

public interface QueryPlanner {
    /**
     * Create a plan for the parsed query.
     */
    public Plan createPlan(QueryData data, Transaction tx);
}
