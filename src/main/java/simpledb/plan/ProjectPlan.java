package simpledb.plan;

import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {
    private Plan p;
    private Schema schema = new Schema();

    public ProjectPlan(Plan p, List<String> fieldList) {
        this.p = p;
        for (String fieldName : fieldList) {
            schema.add(fieldName, p.schema());
        }
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    @Override
    public int blockAccessed() {
        return p.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return p.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
