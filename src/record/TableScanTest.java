package record;

import server.SimpleDB;
import tx.Transaction;

public class TableScanTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/tabletest", 400, 8);
        Transaction tx = db.newTx();
        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 9);
        Layout layout = new Layout(sch);
        for (String fldname : layout.schema().fields()) {
            int offset = layout.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }

        TableScan ts = new TableScan(tx, "T", layout);
        System.out.println("Filling the table with 50 random records.");
        ts.beforeFirst();
        for (int i = 0; i < 50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec"+n);
            System.out.println("inserting into slot " + ts.getRid() + ": {" + n + ", rec" + n + "}");
        }
        System.out.println("Deleting records with A-value < 25.");
        int count = 0;
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            if (a < 25) {
                count++;
                System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b +  "}");
                ts.delete();
            }
        }
        System.out.println(count + " values under 25 were deleted.\n");

        System.out.println("Here are the remaining records");
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b +  "}");
        }

        ts.close();
        tx.commit();
    }
}
