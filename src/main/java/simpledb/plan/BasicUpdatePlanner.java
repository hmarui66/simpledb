package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.parse.*;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner {
    private MetadataMgr mdm;

    public BasicUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        p = new SelectPlan(p, data.pred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
            Constant val = data.newVal().evaluate(us);
            us.setVal(data.targetField(), val);
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), mdm);
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        Iterator<Constant> iter = data.vals().iterator();
        for (String fieldName : data.fields()) {
            Constant val = iter.next();
            us.setVal(fieldName, val);
        }
        us.close();
        return 1;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction tx) {
        mdm.createTable(data.tableName(), data.newSchema(), tx);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction tx) {
        mdm.createView(data.viewName(), data.viewDef(), tx);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        mdm.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;
    }
}
