package simpledb.record.rowlock

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.server.SimpleDB
import simpledb.tx.rowlock.TransactionImpl
import java.io.File
import kotlin.concurrent.thread
import kotlin.math.roundToInt

class TableScanTest {
    // layout definition (A int, B varchar(9))
    private val layout = Layout(Schema().apply {
        this.addIntField("A")
        this.addStringField("B", 9)
    })

    @BeforeEach
    fun before() {
        File("dbdir", "tableScanTest").deleteRecursively()
    }

    @Test
    fun insertAndNextAll() {
        val db = SimpleDB("dbdir/tableScanTest", 400, 8)
        val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

        val ts = TableScan(tx, "T", layout)

        // insert operation
        val inserted = mutableListOf<Int>()
        ts.beforeFirst()
        repeat(50) {
            ts.insert()
            val n = (Math.random() * 50).roundToInt()
            inserted.add(n)
            ts.setInt("A", n)
            ts.setString("B", "rec$n")
        }

        // read operation
        ts.beforeFirst()
        var idx = 0
        while (ts.next()) {
            val n = inserted[idx++]
            val a = ts.getInt("A")
            val b = ts.getString("B")
            assertThat(a, `is`(n))
            assertThat(b, `is`("rec$n"))
        }

        ts.close()
        tx.rollback()
    }

    @Nested
    inner class Concurrent1 {
        private val aValues = listOf(1, 5, 10)
        private lateinit var db: SimpleDB

        @BeforeEach
        fun before() {
            db = SimpleDB("dbdir/tableScanTest", 400, 8)
            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

            val ts = TableScan(tx, "T", layout)

            // insert operation
            ts.beforeFirst()
            for (aValue in aValues) {
                ts.insert()
                ts.setInt("A", aValue)
                ts.setString("B", "rec$aValue")
            }

            ts.close()
            tx.commit()
        }

        @Test
        fun nextSucceedWhenSLockAcquiredByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()

            // read and lock shared all records
            var idx = 0
            while (tsByOtherTx.next()) {
                val a = tsByOtherTx.getInt("A")
                val aValue = aValues[idx++]
                assertThat(a, `is`(aValue))
            }

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            val t = thread {
                var idxInT = 0
                while (ts.next()) {
                    val aValue = aValues[idxInT++]
                    val a = ts.getInt("A")
                    assertThat(a, `is`(aValue))
                }
            }
            t.join(10)
            assertThat(t.isAlive, `is`(false))

            tsByOtherTx.close()
            otherTx.rollback()

            ts.close()
            tx.rollback()
        }

        @Test
        fun setStringSucceedWhenSLockAcquiredAndReleaseByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()

            // read and lock shared all records
            var idx = 0
            while (tsByOtherTx.next()) {
                val a = tsByOtherTx.getInt("A")
                val aValue = aValues[idx++]
                assertThat(a, `is`(aValue))
            }

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()

            // check read and write first row
            assertThat(ts.next(), `is`(true))
            assertThat(ts.setString("B", "new b value"), `is`(false))

            while (ts.next()) {
                assertThat(ts.setString("B", "new b value"), `is`(false))
            }

            tsByOtherTx.close()
            otherTx.rollback()

            ts.beforeFirst()

            // check read and write first row
            assertThat(ts.next(), `is`(true))
            assertThat(ts.setString("B", "new b value"), `is`(true))
            while (ts.next()) {
                assertThat(ts.setString("B", "new b value"), `is`(true))
            }

            ts.beforeFirst()
            idx = 0
            while (ts.next()) {
                assertThat(ts.getInt("A"), `is`(aValues[idx++]))
                assertThat(ts.getString("B"), `is`("new b value"))
            }

            ts.close()
            tx.rollback()
        }

        @Test
        fun nextSucceedWhenXLockAcquiredAndReleasedByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()

            // lock exclusive all records
            while (tsByOtherTx.next()) {
                tsByOtherTx.setString("B", "new b value")
            }

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThat(ts.next(), `is`(false))

            // commit by otherTx
            tsByOtherTx.close()
            otherTx.commit()

            ts.beforeFirst()
            var idx = 0
            while (ts.next()) {
                assertThat(ts.getInt("A"), `is`(aValues[idx++]))
                assertThat(ts.getString("B"), `is`("new b value"))
            }

            ts.close()
            tx.rollback()
        }
    }
}
