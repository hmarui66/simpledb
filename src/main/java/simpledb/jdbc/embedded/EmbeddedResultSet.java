package simpledb.jdbc.embedded;

import simpledb.jdbc.ResultSetAdapter;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class EmbeddedResultSet extends ResultSetAdapter {
    private Scan s;
    private Schema sch;
    private EmbeddedConnection conn;

    public EmbeddedResultSet(Plan plan, EmbeddedConnection conn) {
        s = plan.open();
        sch = plan.schema();
        this.conn = conn;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return s.next();
        } catch (RuntimeException e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    public int getInt(String fldname) throws SQLException {
        try {
            fldname = fldname.toLowerCase(); // to ensure case-insensitivity
            return s.getInt(fldname);
        }
        catch(RuntimeException e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    public String getString(String fldname) throws SQLException {
        try {
            fldname = fldname.toLowerCase(); // to ensure case-insensitivity
            return s.getString(fldname);
        }
        catch(RuntimeException e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return new EmbeddedMetaData(sch);
    }

    @Override
    public void close() throws SQLException {
        s.close();
        conn.commit();
    }
}
