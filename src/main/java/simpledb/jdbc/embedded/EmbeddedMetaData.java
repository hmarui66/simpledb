package simpledb.jdbc.embedded;

import simpledb.jdbc.ResultSetMetaDataAdapter;
import simpledb.record.Schema;

import java.sql.SQLException;

import static java.sql.Types.INTEGER;

public class EmbeddedMetaData extends ResultSetMetaDataAdapter {
    private Schema sch;
    public EmbeddedMetaData(Schema sch) {
        this.sch = sch;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return sch.fields().size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return sch.fields().get(column-1);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String fldName = getColumnName(column);
        return sch.type(fldName);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        String fldName = getColumnName(column);
        int fldType = sch.type(fldName);
        int fldLength = (fldType == INTEGER) ? 6 : sch.length(fldName);
        return Math.max(fldName.length(), fldLength) + 1;
    }
}
