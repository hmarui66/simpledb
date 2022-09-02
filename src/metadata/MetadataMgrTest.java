package metadata;

import record.Layout;
import record.Schema;
import record.TableScan;
import server.SimpleDB;
import tx.Transaction;

import java.util.Map;

import static java.sql.Types.INTEGER;

public class MetadataMgrTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/metadatamgrtest", 400, 8);
        Transaction tx = db.newTx();
        MetadataMgr mdm = new MetadataMgr(true, tx);

        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 9);

        // Part 1: Table Metadata
        mdm.createTable("MyTable", sch, tx);
        Layout layout = mdm.getLayout("MyTable", tx);
        int size = layout.slotSize();
        Schema sch2 = layout.schema();
        System.out.println("MyTable has slot size " + size);
        System.out.println("Its fields are:");
        for (String fieldName : sch2.fields()) {
            String type;
            if (sch2.type(fieldName) == INTEGER)
                type = "int";
            else {
                int strlen = sch2.length(fieldName);
                type = "varchar(" + strlen + ")";
            }
            System.out.println(fieldName + ": " + type);
        }

        // Part 2: Statistics Metadata
        TableScan ts = new TableScan(tx, "MyTable", layout);
        for (int i = 0; i<50; i++) {
            ts.insert();
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec"+n);
        }
        StatInfo si = mdm.getStatInfo("MyTable", layout, tx);
        System.out.println("B(MyTable) = " + si.blocksAccessed());
        System.out.println("R(MyTable) = " + si.recordsOutput());
        System.out.println("V(MyTable, A) = " + si.distinctValues("A"));
        System.out.println("V(MyTable, B) = " + si.distinctValues("B"));

        // Part 3: View Metadata
        String viewDef = "select B from MyTable where A = 1";
        mdm.createView("viewA", viewDef, tx);
        String v = mdm.getViewDef("viewA", tx);
        System.out.println("View def = " + v);

        // Part 4: Index Metadata
        mdm.createIndex("indexA", "MyTable", "A", tx);
        mdm.createIndex("indexB", "MyTable", "B", tx);
        Map<String, IndexInfo> idxMap = mdm.getIndexInfo("MyTable", tx);

        IndexInfo ii = idxMap.get("A");
        System.out.println("B(indexA) = " + ii.blocksAccessed());
        System.out.println("R(indexA) = " + ii.recordsOutput());
        System.out.println("V(indexA, A) = " + ii.distinctValues("A"));
        System.out.println("V(indexA, B) = " + ii.distinctValues("B"));

        ii = idxMap.get("B");
        System.out.println("B(indexB) = " + ii.blocksAccessed());
        System.out.println("R(indexB) = " + ii.recordsOutput());
        System.out.println("V(indexB, A) = " + ii.distinctValues("A"));
        System.out.println("V(indexB, B) = " + ii.distinctValues("B"));
    }
}
