package simpledb.record;

import simpledb.file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;

public class Layout {
    private Schema schema;
    private Map<String, Integer> offsets;
    private int slotsize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES; // space for the empty/inuse flag
        for (String fldname: schema.fields()) {
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }
        slotsize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotsize = slotsize;
    }

    public int offset(String fldname) {
        return offsets.get(fldname);
    }

    public int slotSize() {
        return slotsize;
    }

    private int lengthInBytes(String fldname) {
        int fldtype = schema.type(fldname);
        if (fldtype == INTEGER) {
            return Integer.BYTES;
        } else { // fldtype == VARCHAR
            return Page.maxLength(schema.length(fldname));
        }
    }

    public Schema schema() {
        return schema;
    }
}
