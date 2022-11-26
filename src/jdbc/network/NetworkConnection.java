package jdbc.network;

import jdbc.ConnectionAdapter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class NetworkConnection extends ConnectionAdapter {
    private RemoteConnection rConn;
    public NetworkConnection(RemoteConnection rConn) {
        this.rConn = rConn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        try {
            RemoteStatement rStmt = rConn.createStatement();
            return new NetworkStatement(rStmt);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            rConn.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
