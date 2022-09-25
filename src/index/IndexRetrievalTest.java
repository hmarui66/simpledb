package index;

import metadata.IndexInfo;
import metadata.MetadataMgr;
import plan.Plan;
import plan.TablePlan;
import query.Constant;
import query.UpdateScan;
import record.RID;
import server.SimpleDB;
import tx.Transaction;

import java.util.Map;

public class IndexRetrievalTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/studentdb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mgMgr();

        // Open a scan on the data table.
        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        // Open the index on MajorId
        Map<String, IndexInfo> indexes = mdm.getIndexInfo("student", tx);
        IndexInfo ii = indexes.get("majorId");
        Index idx = ii.open();

        // Retrieve all index records having a dataVal of 20.
        idx.beforeFirst(new Constant(20));
        while (idx.next()) {
            // Use the dataRid to go to the corresponding STUDENT record.
            RID dataRid = idx.getDataRid();
            studentScan.moveToRid(dataRid);
            System.out.println(studentScan.getString("sName"));
        }

        // Close the index and the data table.
        idx.close();
        studentScan.close();
        tx.commit();
    }
}
