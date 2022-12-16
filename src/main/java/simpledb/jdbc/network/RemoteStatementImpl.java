package simpledb.jdbc.network;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
    private RemoteConnectionImpl rConn;
    private Planner planner;

    public RemoteStatementImpl(RemoteConnectionImpl rConn, Planner planner) throws RemoteException {
        this.rConn = rConn;
        this.planner = planner;
    }

    @Override
    public RemoteResultSet executeQuery(String query) throws RemoteException {
        try {
            Transaction tx = rConn.getTransaction();
            Plan plan = planner.createQueryPlan(query, tx);
            return new RemoteResultSetImpl(plan, rConn);
        } catch (RuntimeException e) {
            rConn.rollback();
            throw e;
        }
    }

    @Override
    public int executeUpdate(String cmd) throws RemoteException {
        try {
            Transaction tx = rConn.getTransaction();
            int result = planner.executeUpdate(cmd, tx);
            rConn.commit();
            return result;
        } catch (RuntimeException e) {
            rConn.rollback();
            throw e;
        }
    }

    @Override
    public void close() throws RemoteException {
    }
}
