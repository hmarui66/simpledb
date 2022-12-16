package simpledb.jdbc.network;

import simpledb.jdbc.embedded.DriverAdapter;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class NetworkDriver extends DriverAdapter {
    public Connection connect(String url, Properties prop) throws SQLException {
        try {
            String host = url.replace("jdbc:simpledb://", "");
            Registry reg = LocateRegistry.getRegistry(host, 1099);
            RemoteDriver rDrv = (RemoteDriver) reg.lookup("simpledb");
            RemoteConnection rConn = rDrv.connect();
            return new NetworkConnection(rConn);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
