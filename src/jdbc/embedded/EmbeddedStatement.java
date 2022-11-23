package jdbc.embedded;

import jdbc.StatementAdapter;
import plan.Plan;
import plan.Planner;
import tx.Transaction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EmbeddedStatement extends StatementAdapter {
    private EmbeddedConnection conn;
    private Planner planner;


    public EmbeddedStatement(EmbeddedConnection conn, Planner planner) {
        this.conn = conn;
        this.planner = planner;
    }

    @Override
    public EmbeddedResultSet executeQuery(String sql) throws SQLException {
        try {
            Transaction tx = conn.getTransaction();
            Plan plan = planner.createQueryPlan(sql, tx);
            return new EmbeddedResultSet(plan, conn);
        } catch (RuntimeException e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            Transaction tx = conn.getTransaction();
            int result = planner.executeUpdate(sql, tx);
            conn.commit();
            return result;
        } catch (RuntimeException e) {
            conn.rollback();
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
    }
}
