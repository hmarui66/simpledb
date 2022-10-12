package index;

import metadata.IndexInfo;
import metadata.MetadataMgr;
import plan.Plan;
import plan.Planner;
import plan.TablePlan;
import query.Constant;
import query.UpdateScan;
import record.RID;
import server.SimpleDB;
import tx.Transaction;

import java.util.Map;

public class CreateStudentDb {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/studentDb");
        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();
        String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
        Planner planner = db.planner();
        planner.executeUpdate(s, tx);
        System.out.println("Table STUDENT created.");

        s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
        String[] students = {"(1, 'joe', 10, 2021)",
                "(2, 'amy', 20, 2020)",
                "(3, 'max', 10, 2022)",
                "(4, 'sue', 20, 2022)",
                "(5, 'bob', 30, 2020)",
                "(6, 'kim', 20, 2020)",
                "(7, 'art', 30, 2021)",
                "(8, 'pat', 20, 2019)",
                "(9, 'lee', 10, 2021)"};
        for (String val : students) planner.executeUpdate(s + val, tx);
        System.out.println("STUDENT records inserted.");

//        s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
//        planner.executeUpdate(s, tx);
//        System.out.println("Table ENROLL created.");
//
//        s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
//        String[] enrolls = {"(14, 1, 13, 'A')",
//                "(24, 1, 43, 'C' )",
//                "(34, 2, 43, 'B+')",
//                "(44, 4, 33, 'B' )",
//                "(54, 4, 53, 'A' )",
//                "(64, 6, 53, 'A' )"};
//        for (String val : enrolls) {
//            planner.executeUpdate(s + val, tx);
//        }
//        System.out.println("ENROLL records inserted.");

        mdm.createIndex("majorIndex", "student", "majorid", tx);
        System.out.println("majorIndex created.");
        Map<String, IndexInfo> idxInfo = mdm.getIndexInfo("student", tx);
        Index idx = idxInfo.get("majorid").open();

        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        studentScan.beforeFirst();
        while (studentScan.next()) {
            Constant val = studentScan.getVal("majorid");
            RID rid = studentScan.getRid();
            idx.insert(val, rid);
        }
        System.out.println("majorIndex inserted.");

        tx.commit();
    }
}
