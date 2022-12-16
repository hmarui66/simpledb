package simpledb.plan;

import simpledb.parse.*;
import simpledb.tx.Transaction;

public interface UpdatePlanner {
    /**
     * Executes the specified insert statement, and
     * returns the number of affected records.
     */
    public int executeInsert(InsertData data, Transaction tx);

    /**
     * Executes the specified delete statement, and
     * returns the number of affected records.
     */
    public int executeDelete(DeleteData data, Transaction tx);

    /**
     * Executes the specified modify statement, and
     * returns the number of affected records.
     */
    public int executeModify(ModifyData data, Transaction tx);

    /**
     * Executes the specified create table statement, and
     * returns the number of affected records.
     */
    public int executeCreateTable(CreateTableData data, Transaction tx);

    /**
     * Executes the specified create view statement, and
     * returns the number of affected records.
     */
    public int executeCreateView(CreateViewData data, Transaction tx);

    /**
     * Executes the specified create index statement, and
     * returns the number of affected records.
     */
    public int executeCreateIndex(CreateIndexData data, Transaction tx);
}
