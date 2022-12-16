package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BetterQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BetterQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblName : data.tables()) {
            String viewDef = mdm.getViewDef(tblName, tx);
            if (viewDef != null) { // Recursively plan the view.
                Parser parser = new Parser(viewDef);
                QueryData viewData = parser.query();
                plans.add(createPlan(viewData, tx));
            } else {
                plans.add(new TablePlan(tx, tblName, mdm));
            }
        }

        // Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextPlan : plans) {
            // Try both orderings and choose the one having the lowest cost
            Plan choice1 = new ProductPlan(nextPlan, p);
            Plan choice2 = new ProductPlan(p, nextPlan);
            if (choice1.blockAccessed() < choice2.blockAccessed())
                p = choice1;
            else
                p = choice2;
        }

        // Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        // Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
