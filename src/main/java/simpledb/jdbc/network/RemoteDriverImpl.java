package simpledb.jdbc.network;

import simpledb.server.SimpleDB;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteDriverImpl extends UnicastRemoteObject implements RemoteDriver {
    private SimpleDB db;

    public RemoteDriverImpl(SimpleDB db) throws RemoteException {
        this.db = db;
    }

    public RemoteConnection connect() throws RemoteException {
        return new RemoteConnectionImpl(db);
    }
}
