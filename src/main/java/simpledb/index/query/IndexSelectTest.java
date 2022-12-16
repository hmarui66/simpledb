package simpledb.index.query;

import simpledb.index.Index;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.Map;

public class IndexSelectTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/studentDb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();

        // Find the index on StudentId.
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("enroll", tx);
        IndexInfo sidIdx = indexes.get("sid");

        // Get the plan for the Enroll table
        Plan enrollPlan = new TablePlan(tx, "enroll", mdm);

        // Create the selection constant
        Constant c = new Constant(6);

        // Two different ways to use the index in simpledb:
        useIndexManually(sidIdx, enrollPlan, c);
        useIndexScan(sidIdx, enrollPlan, c);

        tx.commit();
    }

    private static void useIndexManually(IndexInfo ii, Plan p, Constant c) {
        // Open a scan on the table.
        TableScan s = (TableScan) p.open();
        Index idx = ii.open();

        // Retrieve all index records having the specified dataVal.
        idx.beforeFirst(c);
        while (idx.next()) {
            // Use the dataRid to go to the corresponding to Enroll record.
            RID dataRid = idx.getDataRid();
            s.moveToRid(dataRid);
            System.out.println(s.getString("grade"));
        }
        idx.close();
        s.close();
    }

    private static void useIndexScan(IndexInfo ii, Plan p, Constant c) {
        // Open an index select scan on the enroll table.
        Plan idxPlan = new IndexSelectPlan(p, ii, c);
        Scan s = idxPlan.open();

        while (s.next())
            System.out.println(s.getString("grade"));
        s.close();
    }
}
