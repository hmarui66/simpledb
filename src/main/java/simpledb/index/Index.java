package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

public interface Index {
    /**
     * Positions the index before the first record
     * having the specified search key.
     */
    public void beforeFirst(Constant searchKey);

    /**
     * Moves the index to the next record having the
     * search key specified in the beforeFirst method.
     * Returns false if there are no more such index records.
     */
    public boolean next();

    /**
     * Returns the dataRID value stored in the current index record.
     */
    public RID getDataRid();

    /**
     * Inserts an index record having the specified
     * dataVal and dataRID values.
     */
    public void insert(Constant dataVal, RID dataRid);

    /**
     * Deletes the index record having the specified
     * dataVal and dataRID values.
     */
    public void delete(Constant dataVal, RID dataRid);

    public void close();
}
