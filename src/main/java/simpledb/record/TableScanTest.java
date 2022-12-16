package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

@SuppressWarnings("DuplicatedCode")
public class TableScanTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("dbdir/tabletest", 400, 8);
        // トランザクション開始
        Transaction tx = db.newTx();

        // スキーマ定義(A int, B varchar(9))
        Schema schema = new Schema();
        schema.addIntField("A");
        schema.addStringField("B", 9);
        Layout layout = new Layout(schema);

        // テーブルスキャンクラス(読み込みだけでなく更新操作もハンドリング)
        TableScan ts = new TableScan(tx, "T", layout);

        // インサート操作
        ts.beforeFirst();
        for (int i = 0; i < 50; i++) {
            ts.insert(); // レコード挿入先の slot へ移動
            int n = (int) Math.round(Math.random() * 50);
            ts.setInt("A", n);
            ts.setString("B", "rec"+n);
        }

        // 削除操作
        int count = 0;
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            if (a < 25) {
                count++;
                System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b +  "}");
                ts.delete();
            }
        }

        // 読み込み操作
        ts.beforeFirst();
        while (ts.next()) {
            int a = ts.getInt("A");
            String b = ts.getString("B");
            System.out.println("slot " + ts.getRid() + ": {" + a + ", " + b +  "}");
        }

        ts.close();
        tx.commit();
    }
}
