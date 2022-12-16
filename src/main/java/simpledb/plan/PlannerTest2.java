package simpledb.plan;

import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class PlannerTest2 {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/plannertest1");
        Transaction tx = db.newTx();
        Planner planner = db.planner();
        String cmd = "create table T1(A int, B varchar(9))";
        planner.executeUpdate(cmd, tx);

        int n = 200;
        System.out.println("Inserting " + n + " random records into T1.");
        for (int i = 0; i < n; i++) {
            int a = (int) Math.round(Math.random() * 50);
            String b = "bbb" + a + "(idx=" + i + ")";
            cmd = "insert into T1(A, B) values(" + a + ", '" + b + "')";
            planner.executeUpdate(cmd, tx);
        }

        cmd = "create table T2(C int, D varchar(9))";
        planner.executeUpdate(cmd, tx);
        System.out.println("Inserting " + n + " records into T2.");
        for (int i = 0; i < n; i++) {
            int c = n - i -1;
            String d = "ddd" + c + "(idx=" + i + ")";
            cmd = "insert into T2(C,D) values(" + c + ", '" + d + "')";
            planner.executeUpdate(cmd, tx);
        }

        String query = "select B,D from T1,T2 where A=C";
        Plan p = planner.createQueryPlan(query, tx);
        Scan s = p.open();
        while (s.next())
            System.out.println(s.getString("b") + " " + s.getString("d"));
        s.close();
        tx.commit();
    }
}
