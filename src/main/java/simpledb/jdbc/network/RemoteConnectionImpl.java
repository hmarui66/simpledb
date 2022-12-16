package simpledb.jdbc.network;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private SimpleDB db;
    private Transaction currentTx;
    private Planner planner;
    public RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        currentTx = db.newTx();
        planner = db.planner();
    }

    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    @Override
    public void close() throws RemoteException {
        currentTx.commit();
    }

    Transaction getTransaction() {
        return currentTx;
    }

    void commit() {
        currentTx.commit();
        currentTx = db.newTx();
    }

    void rollback() {
        currentTx.rollback();
        currentTx = db.newTx();
    }
}
