package simpledb.index;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.HashMap;
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

        s = "create table ENROLL(EId int, SId int, SectionId int, Grade varchar(2))";
        planner.executeUpdate(s, tx);
        System.out.println("Table ENROLL created.");

        s = "insert into ENROLL(EId, SId, SectionId, Grade) values ";
        String[] enrolls = {"(14, 1, 13, 'A')",
                "(24, 1, 43, 'C' )",
                "(34, 2, 43, 'B+')",
                "(44, 4, 33, 'B' )",
                "(54, 4, 53, 'A' )",
                "(64, 6, 53, 'A' )"};
        for (String val : enrolls) {
            planner.executeUpdate(s + val, tx);
        }
        System.out.println("ENROLL records inserted.");

        mdm.createIndex("studentSidIndex", "student", "sid", tx);
        System.out.println("studentSidIndex created.");
        mdm.createIndex("studentMajorIndex", "student", "majorid", tx);
        System.out.println("studentMajorIndex created.");
        mdm.createIndex("enrollSidIndex", "enroll", "sid", tx);
        System.out.println("enrollSidIndex created.");

        insertStudentIndex(tx, mdm);
        insertEnrollIndex(tx, mdm);

        tx.commit();
    }

    private static void insertStudentIndex(Transaction tx, MetadataMgr mdm) {
        Map<String,Index> indexes = new HashMap<>();
        Map<String, IndexInfo> idxInfo = mdm.getIndexInfo("student", tx);

        for (String fieldName : idxInfo.keySet()) {
            Index idx = idxInfo.get(fieldName).open();
            indexes.put(fieldName, idx);
        }

        Plan studentPlan = new TablePlan(tx, "student", mdm);
        UpdateScan studentScan = (UpdateScan) studentPlan.open();

        studentScan.beforeFirst();
        while (studentScan.next()) {
            RID rid = studentScan.getRid();
            for (String fieldName : indexes.keySet()) {
                Constant val = studentScan.getVal(fieldName);
                Index idx = indexes.get(fieldName);
                idx.insert(val, rid);
            }
        }
        System.out.println("Student Index inserted.");
    }
    private static void insertEnrollIndex(Transaction tx, MetadataMgr mdm) {
        Map<String,Index> indexes = new HashMap<>();
        Map<String, IndexInfo> idxInfo = mdm.getIndexInfo("enroll", tx);

        for (String fieldName : idxInfo.keySet()) {
            Index idx = idxInfo.get(fieldName).open();
            indexes.put(fieldName, idx);
        }

        Plan studentPlan = new TablePlan(tx, "enroll", mdm);
        UpdateScan enrollScan = (UpdateScan) studentPlan.open();

        enrollScan.beforeFirst();
        while (enrollScan.next()) {
            RID rid = enrollScan.getRid();
            for (String fieldName : indexes.keySet()) {
                Constant val = enrollScan.getVal(fieldName);
                Index idx = indexes.get(fieldName);
                idx.insert(val, rid);
            }
        }
        System.out.println("Enroll Index inserted.");
    }
}
