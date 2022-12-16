package simpledb.jdbc.network;

import simpledb.jdbc.ResultSetAdapter;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class NetworkResultSet extends ResultSetAdapter {
    private RemoteResultSet rRs;

    public NetworkResultSet(RemoteResultSet rRs) {
        this.rRs = rRs;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return rRs.next();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
        try {
            return rRs.getInt(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        try {
            return rRs.getString(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            RemoteMetaData rmd = rRs.getMetaData();
            return new NetworkMetaData(rmd);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
