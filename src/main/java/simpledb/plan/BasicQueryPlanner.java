package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BasicQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
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
        for (Plan nextPlan : plans)
            p = new ProductPlan(p, nextPlan);

        // Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        // Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
