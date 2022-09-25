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

import java.util.HashMap;
import java.util.Map;

public class IndexUpdateTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/studentdb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mgMgr();
        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        // Create a map containing all indexes for STUDENT
        Map<String, Index> indexes = new HashMap<>();
        Map<String , IndexInfo> idxInfo = mdm.getIndexInfo("student", tx);
        for (String fieldName : idxInfo.keySet()) {
            Index idx = idxInfo.get(fieldName).open();
            indexes.put(fieldName, idx);
        }

        // Task 1: insert a new STUDENT record for Sam
        //    First, insert the record into STUDENT.
        studentScan.insert();
        studentScan.setInt("sid", 11);
        studentScan.setString("sName", "sam");
        studentScan.setInt("gradYear", 2023);
        studentScan.setInt("majorId", 30);

        // Then insert a record into each of the indexes.
        RID dataRid = studentScan.getRid();
        for (String fieldName: indexes.keySet()) {
            Constant dataVal = studentScan.getVal(fieldName);
            Index idx = indexes.get(fieldName);
            idx.insert(dataVal, dataRid);
        }

        // Task 2: find and delete Joe's record
        studentScan.beforeFirst();
        while (studentScan.next()) {
            if (studentScan.getString("sName").equals("joe")) {

                // First, delete the index records for Joe
                RID joeRid = studentScan.getRid();
                for (String  fieldName : indexes.keySet()) {
                    Constant dataVal = studentScan.getVal(fieldName);
                    Index idx = indexes.get(fieldName);
                    idx.delete(dataVal, joeRid);
                }

                // Then delete Joe's record in STUDENT
                studentScan.delete();
                break;
            }
        }

        // Print the records to verify the updates
        studentScan.beforeFirst();
        while (studentScan.next())
            System.out.println(studentScan.getString("sName") + " " + studentScan.getInt("sid"));
        studentScan.close();

        for (Index idx : indexes.values())
            idx.close();
        tx.commit();
    }
}
