package simpledb.plan;

import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class PlannerTest1 {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/plannertest1");
        Transaction tx = db.newTx();
        Planner planner = db.planner();
        String cmd = "create table T1(A int, B varchar(9))";
        planner.executeUpdate(cmd, tx);

        int n = 200;
        System.out.println("Inserting " + n + " random records.");
        for (int i = 0; i < n; i++) {
            int a = (int) Math.round(Math.random() * 50);
            String b = "rec" + a + "(idx=" + i + ")";
            cmd = "insert into T1(A, B) values(" + a + ", '" + b + "')";
            System.out.println(cmd);
            planner.executeUpdate(cmd, tx);
        }

        String query = "select B from T1 where A=10";
        Plan p = planner.createQueryPlan(query, tx);
        Scan s = p.open();
        while (s.next())
            System.out.println(s.getString("b"));
        s.close();
        tx.commit();
    }
}
