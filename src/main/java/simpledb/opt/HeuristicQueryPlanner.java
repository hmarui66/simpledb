package simpledb.opt;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.Plan;
import simpledb.plan.ProjectPlan;
import simpledb.plan.QueryPlanner;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 */
public class HeuristicQueryPlanner implements QueryPlanner {
    private Collection<TablePlanner> tablePlanners = new ArrayList<>();
    private MetadataMgr mdm;

    public HeuristicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    /**
     * Creates an optimized left-deep query plan using the following
     * heuristics.
     * H1. Choose the smallest table (considering selection predicates)
     * to be first in the join order.
     * H2. Add the table to the join order which
     * results in the smallest output.
     */
    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: それぞれのテーブルの TablePlanner を生成
        for (String tblName : data.tables()) {
            TablePlanner tp = new TablePlanner(tblName, data.pred(), tx, mdm);
            tablePlanners.add(tp);
        }

        // Step 2: lowest-size プランを選択
        Plan currentPlan = getLowestSelectPlan();

        // Step 3: join order に繰り返しプランを追加
        while (!tablePlanners.isEmpty()) {
            Plan p = getLowestJoinPlan(currentPlan);
            if (p != null) {
                currentPlan = p;
            } else {
                currentPlan = getLowestProductPlan(currentPlan);
            }
        }

        // Step 4: Project on the field names and return
        return new ProjectPlan(currentPlan, data.fields());
    }

    private Plan getLowestSelectPlan() {
        TablePlanner bestTp = null;
        Plan bestPlan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeSelectPlan();
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTp = tp;
                bestPlan = plan;
            }
        }
        if (bestPlan != null) {
            tablePlanners.remove(bestTp);
        }
        return bestPlan;
    }

    private Plan getLowestJoinPlan(Plan current) {
        TablePlanner bestTp = null;
        Plan bestPlan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeJoinPlan(current);
            if (plan != null && (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput())) {
                bestTp = tp;
                bestPlan = plan;
            }
        }
        if (bestPlan != null) {
            tablePlanners.remove(bestTp);
        }
        return bestPlan;
    }

    private Plan getLowestProductPlan(Plan current) {
        TablePlanner bestTp = null;
        Plan bestPlan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeProductPlan(current);
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTp = tp;
                bestPlan = plan;
            }
        }
        if (bestPlan != null) {
            tablePlanners.remove(bestTp);
        }
        return bestPlan;
    }
}
