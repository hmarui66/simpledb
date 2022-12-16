package simpledb.jdbc.embedded;

import simpledb.server.SimpleDB;

import java.sql.SQLException;
import java.util.Properties;

public class EmbeddedDriver extends DriverAdapter {
    @Override
    public EmbeddedConnection connect(String url, Properties info) throws SQLException {
        String dbName = url.replace("jdbc:simpledb:", "");
        SimpleDB db = new SimpleDB(dbName);
        return new EmbeddedConnection(db);
    }
}
