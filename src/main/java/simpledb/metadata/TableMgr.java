package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableMgr {
    public static final int MAX_NAME = 16; // table or field name
    private Layout tableCatLayout, fieldCatLayout;

    public TableMgr(boolean isNew, Transaction tx) {
        Schema tableCatSchema = new Schema();
        tableCatSchema.addStringField("tableName", MAX_NAME);
        tableCatSchema.addIntField("slotSize");
        tableCatLayout = new Layout(tableCatSchema);

        Schema fieldCatSchema = new Schema();
        fieldCatSchema.addStringField("tableName", MAX_NAME);
        fieldCatSchema.addStringField("fieldName", MAX_NAME);
        fieldCatSchema.addIntField("type");
        fieldCatSchema.addIntField("length");
        fieldCatSchema.addIntField("offset");
        fieldCatLayout = new Layout(fieldCatSchema);

        if (isNew) {
            createTable("tableCatalog", tableCatSchema, tx);
            createTable("fieldCatalog", fieldCatSchema, tx);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        // insert one record into tableCatalog
        TableScan tableCatalog = new TableScan(tx, "tableCatalog", tableCatLayout);
        tableCatalog.insert();
        tableCatalog.setString("tableName", tableName);
        tableCatalog.setInt("slotSize", layout.slotSize());
        tableCatalog.close();

        // insert a record into fieldCatalog for each field
        TableScan fieldCatalog = new TableScan(tx, "fieldCatalog", fieldCatLayout);
        for (String fieldName: schema.fields()) {
            fieldCatalog.insert();
            fieldCatalog.setString("tableName", tableName);
            fieldCatalog.setString("fieldName", fieldName);
            fieldCatalog.setInt("type", schema.type(fieldName));
            fieldCatalog.setInt("length", schema.length(fieldName));
            fieldCatalog.setInt("offset", layout.offset(fieldName));
        }
        fieldCatalog.close();
    }

    public Layout getLayout(String tableName, Transaction tx) {
        int size = -1;
        TableScan tableCatalog = new TableScan(tx, "tableCatalog", tableCatLayout);
        while (tableCatalog.next()) {
            if (tableCatalog.getString("tableName").equals(tableName)) {
                size = tableCatalog.getInt("slotSize");
                break;
            }
        }
        tableCatalog.close();

        Schema sch = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScan fieldCatalog = new TableScan(tx, "fieldCatalog", fieldCatLayout);
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString("tableName").equals(tableName)) {
                String fieldName = fieldCatalog.getString("fieldName");
                int fieldType = fieldCatalog.getInt("type");
                int fieldLength = fieldCatalog.getInt("length");
                int offset = fieldCatalog.getInt("offset");
                offsets.put(fieldName, offset);
                sch.addField(fieldName, fieldType, fieldLength);
            }
        }
        fieldCatalog.close();
        return new Layout(sch, offsets, size);
    }
}
