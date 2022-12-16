package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import static java.sql.Types.INTEGER;

public class TableMgrTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/tableMgrTest", 400, 8);
        Transaction tx = db.newTx();
        TableMgr tm = new TableMgr(true, tx);

        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 9);
        tm.createTable("MyTable", sch, tx);

        Layout layout = tm.getLayout("MyTable", tx);
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
            System.out.println(fieldName + ":" + type);
        }

        tx.commit();
    }
}
