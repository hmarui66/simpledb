package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
    private Scan s;
    private Schema sch;
    private RemoteConnectionImpl rConn;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rConn) throws RemoteException {
        s = plan.open();
        sch = plan.schema();
        this.rConn = rConn;
    }

    @Override
    public boolean next() throws RemoteException {
        try {
            return s.next();
        } catch (RuntimeException e) {
            rConn.rollback();
            ;
            throw e;
        }
    }

    @Override
    public int getInt(String fieldName) throws RemoteException {
        try {
            fieldName = fieldName.toLowerCase();
            return s.getInt(fieldName);
        } catch (RuntimeException e) {
            rConn.rollback();
            throw e;
        }
    }

    @Override
    public String getString(String fieldName) throws RemoteException {
        try {
            fieldName = fieldName.toLowerCase();
            return s.getString(fieldName);
        } catch (RuntimeException e) {
            rConn.rollback();
            throw e;
        }
    }

    @Override
    public RemoteMetaData getMetaData() throws RemoteException {
        return new RemoteMetaDataImpl(sch);
    }

    @Override
    public void close() throws RemoteException {
        s.close();
        rConn.commit();
    }
}
