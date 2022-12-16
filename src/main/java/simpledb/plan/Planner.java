package simpledb.plan;

import simpledb.parse.*;
import simpledb.tx.Transaction;

public class Planner {
    private QueryPlanner queryPlanner;
    private UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String query, Transaction tx) {
        Parser parser = new Parser(query);
        QueryData data = parser.query();
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        if (data instanceof InsertData)
            return updatePlanner.executeInsert((InsertData) data, tx);
        else if (data instanceof DeleteData)
            return updatePlanner.executeDelete((DeleteData) data, tx);
        else if (data instanceof ModifyData)
            return updatePlanner.executeModify((ModifyData) data, tx);
        else if (data instanceof CreateTableData)
            return updatePlanner.executeCreateTable((CreateTableData) data, tx);
        else if (data instanceof CreateViewData)
            return updatePlanner.executeCreateView((CreateViewData) data, tx);
        else if (data instanceof CreateIndexData)
            return updatePlanner.executeCreateIndex((CreateIndexData) data, tx);
        else
            return 0;
    }
}
