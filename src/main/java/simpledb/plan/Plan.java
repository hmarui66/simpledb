package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public interface Plan {
    public Scan open();

    /**
     * Returns an estimate of the number of block accesses
     */
    public int blockAccessed();

    /**
     * Returns an estimate of the number of records
     */
    public int recordsOutput();

    /**
     * Returns an estimate of the number of distinct values
     */
    public int distinctValues(String fieldName);

    /**
     * Returns the schema of the query.
     */
    public Schema schema();
}
