package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.*;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.plan.UpdatePlanner;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.tx.Transaction;

import java.util.Iterator;
import java.util.Map;

public class IndexUpdatePlanner implements UpdatePlanner {
    private MetadataMgr mdm;

    public IndexUpdatePlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    public int executeInsert(InsertData data, Transaction tx) {
        String tableName = data.tableName();
        Plan p = new TablePlan(tx, tableName, mdm);

        // first, insert the record
        UpdateScan s = (UpdateScan) p.open();
        s.insert();
        RID rid = s.getRid();

        // then modify each field, inserting an index record if appropriate
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tableName, tx);
        Iterator<Constant> varIter = data.vals().iterator();
        for (String fieldName : data.fields()) {
            Constant val = varIter.next();
            s.setVal(fieldName, val);

            IndexInfo ii = indexes.get(fieldName);
            if (ii != null) {
                Index idx = ii.open();
                idx.insert(val, rid);
                idx.close();
            }
        }
        s.close();
        return 1;
    }

    public int executeDelete(DeleteData data, Transaction tx) {
        String tableName = data.tableName();
        Plan p = new TablePlan(tx, tableName, mdm);
        p = new SelectPlan(p, data.pred());
        Map<String, IndexInfo> indexes = mdm.getIndexInfo(tableName, tx);

        UpdateScan s = (UpdateScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID of every index
            RID rid = s.getRid();
            for (String fieldName : indexes.keySet()) {
                Constant val = s.getVal(fieldName);
                Index idx = indexes.get(fieldName).open();
                idx.delete(val, rid);
                idx.close();
            }
            // then delete the record
            s.delete();
            count++;
        }
        s.close();
        return count;
    }

    public int executeModify(ModifyData data, Transaction tx) {
        String tableName = data.tableName();
        String fieldName = data.targetField();
        Plan p = new TablePlan(tx, tableName, mdm);
        p = new SelectPlan(p, data.pred());

        IndexInfo ii = mdm.getIndexInfo(tableName, tx).get(fieldName);
        Index idx = (ii == null) ? null : ii.open();

        UpdateScan s = (UpdateScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, update the record
            Constant newVal = data.newVal().evaluate(s);
            Constant oldVal = s.getVal(fieldName);
            s.setVal(data.targetField(), newVal);

            // then update the appropriate index, if it exists
            if (idx != null) {
                RID rid = s.getRid();
                idx.delete(oldVal, rid);
                idx.insert(newVal, rid);
            }
            count++;
        }
        if (idx != null) idx.close();
        s.close();
        return count;
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
