package simpledb;
import simpledb.jdbc.network.RemoteDriver;
import simpledb.jdbc.network.RemoteDriverImpl;
import simpledb.server.SimpleDB;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class SimpleServer {
    public static void main(String[] args) throws RemoteException {
        String dirName = (args.length == 0) ? "dbdir/studentDb" : args[0];
        SimpleDB db = new SimpleDB(dirName);

        Registry reg = LocateRegistry.createRegistry(1099);

        RemoteDriver d = new RemoteDriverImpl(db);
        reg.rebind("simpledb", d);

        System.out.println("database server ready");
    }
}
