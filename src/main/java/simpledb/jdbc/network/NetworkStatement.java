package simpledb.jdbc.network;

import simpledb.jdbc.StatementAdapter;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NetworkStatement extends StatementAdapter {
    private RemoteStatement rStmt;
    public NetworkStatement(RemoteStatement rStmt) {
        this.rStmt = rStmt;
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        try {
            RemoteResultSet rRs = rStmt.executeQuery(query);
            return new NetworkResultSet(rRs);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String cmd) throws SQLException {
        try {
            return rStmt.executeUpdate(cmd);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            rStmt.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
