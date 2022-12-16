package simpledb.jdbc.network;

import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;

public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
    private Schema sch;
    private List<String> fields = new ArrayList<>();

    public RemoteMetaDataImpl(Schema sch) throws RemoteException {
        this.sch = sch;
        for (String field : sch.fields()) {
            fields.add(field);
        }
    }

    @Override
    public int getColumnCount() throws RemoteException {
        return fields.size();
    }

    @Override
    public String getColumnName(int column) throws RemoteException {
        return fields.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        return sch.type(fieldName);
    }

    @Override
    public int getColumnDisplaySize(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        int fieldType = sch.type(fieldName);
        int fieldLength = (fieldType == INTEGER) ? 6 : sch.length(fieldName);
        return Math.max(fieldName.length(), fieldLength) + 1;
    }
}
