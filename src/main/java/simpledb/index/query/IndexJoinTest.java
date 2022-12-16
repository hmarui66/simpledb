package simpledb.index.query;

import simpledb.index.Index;
import simpledb.index.planner.IndexJoinPlan;
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

public class IndexJoinTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/studentDb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();

        // Find the index on studentId.
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("enroll", tx);
        IndexInfo sidIdx = indexes.get("sid");

        // Get plans for the Student and Enroll tables
        Plan studentPlan = new TablePlan(tx, "student", mdm);
        Plan enrollPlan = new TablePlan(tx, "enroll", mdm);

        // Two different ways to use the index in simpledb:
        useIndexManually(studentPlan, enrollPlan, sidIdx, "sid");
        useIndexScan(studentPlan, enrollPlan, sidIdx, "sid");

        tx.commit();
    }

    private static void useIndexManually(Plan p1, Plan p2, IndexInfo ii, String joinField) {
        Scan s1 = p1.open();
        TableScan s2 = (TableScan) p2.open();
        Index idx = ii.open();

        while (s1.next()) {
            Constant c = s1.getVal(joinField);
            idx.beforeFirst(c);
            while (idx.next()) {
                RID rid = idx.getDataRid();
                s2.moveToRid(rid);
                System.out.println(s2.getString("grade"));
            }
        }
        System.out.println("finish useIndexManually.");
        idx.close();
        s1.close();
        s2.close();
    }

    private static void useIndexScan(Plan p1, Plan p2, IndexInfo ii, String joinField) {
        Plan idxPlan = new IndexJoinPlan(p1, p2, ii, joinField);
        Scan s = idxPlan.open();

        while (s.next()) {
            System.out.println(s.getString("grade"));
        }
        System.out.println("finish useIndexScan.");
        s.close();
    }
}
