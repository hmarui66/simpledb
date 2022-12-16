package simpledb.jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteStatement extends Remote {
    public RemoteResultSet executeQuery(String query) throws RemoteException;
    public int executeUpdate(String cmd) throws RemoteException;
    public void close() throws RemoteException;
}
