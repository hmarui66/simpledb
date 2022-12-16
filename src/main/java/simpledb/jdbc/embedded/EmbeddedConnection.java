package simpledb.jdbc.embedded;

import simpledb.jdbc.ConnectionAdapter;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.sql.SQLException;
import java.sql.Statement;

public class EmbeddedConnection extends ConnectionAdapter {
    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;

    public EmbeddedConnection(SimpleDB db) {
        this.db = db;
        currentTx = db.newTx();
        planner = db.planner();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new EmbeddedStatement(this, planner);
    }

    @Override
    public void close() throws SQLException {
        currentTx.commit();
    }

    @Override
    public void commit() throws SQLException {
        currentTx.commit();
        currentTx = db.newTx();
    }

    @Override
    public void rollback() throws SQLException {
        currentTx.rollback();
        currentTx = db.newTx();
    }

    public Transaction getTransaction() {
        return currentTx;
    }
}
