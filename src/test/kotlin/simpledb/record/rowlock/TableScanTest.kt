package simpledb.record.rowlock

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.server.SimpleDB
import simpledb.tx.concurrency.LockAbortException
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
            assertThrows<LockAbortException> {
                ts.setString("B", "new b value")
            }

            ts.close()
            tx.rollback()

            tsByOtherTx.close()
            otherTx.rollback()

            val tx2 = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts2 = TableScan(tx2, "T", layout)
            ts2.beforeFirst()

            // check read and write first row
            assertThat(ts2.next(), `is`(true))
            assertThat(ts2.setString("B", "new b value"), `is`(true))
            while (ts2.next()) {
                assertThat(ts2.setString("B", "new b value"), `is`(true))
            }

            ts2.close()
            tx2.rollback()
        }

        @Test
        fun setStringSucceedWhenSLockNotAcquiredByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()

            // read and lock shared first record
            tsByOtherTx.next()

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            // proceed to 2nd row
            ts.next()
            ts.next()

            assertThat(ts.getInt("A"), `is`(5))
            assertThat(ts.setString("B", "new value of 5"), `is`(true))

            ts.close()
            tx.rollback()

            tsByOtherTx.close()
            otherTx.rollback()
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
            assertThrows<LockAbortException> {
                ts.next()
            }
            ts.close()
            tx.rollback()

            // commit by otherTx
            tsByOtherTx.close()
            otherTx.commit()

            val tx2 = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts2 = TableScan(tx2, "T", layout)

            var idx = 0
            while (ts2.next()) {
                assertThat(ts2.getInt("A"), `is`(aValues[idx++]))
                assertThat(ts2.getString("B"), `is`("new b value"))
            }

            ts2.close()
            tx2.rollback()
        }
    }

    @Nested
    @DisplayName("Confirm that phantom read does not occur when inserting into an unused slot")
    inner class Concurrent2 {
        private lateinit var db: SimpleDB

        @BeforeEach
        fun before() {
            db = SimpleDB("dbdir/tableScanTest", 400, 8)
            var tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

            var ts = TableScan(tx, "T", layout)

            // insert operation
            ts.beforeFirst()
            ts.insert()
            ts.setInt("A", 1)
            ts.setString("B", "value of 1")
            ts.close()
            tx.commit()

            tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())

            ts = TableScan(tx, "T", layout)

            // delete operation
            ts.beforeFirst()
            ts.next()
            ts.delete()
            ts.close()
            tx.commit()
        }

        @Test
        fun cannotFetchDeletedRow() {
            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThat(ts.next(), `is`(false))

            ts.close()
            tx.rollback()
        }

        @Test
        fun nextFailWhenNewRowInsertedByOtherTx() {
            // insert to unused row
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()
            tsByOtherTx.insert()
            tsByOtherTx.setInt("A", 1)
            tsByOtherTx.setString("B", "new value of 1")

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThrows<LockAbortException> {
                ts.next()
            }

            ts.close()
            tx.rollback()

            tsByOtherTx.close()
            otherTx.rollback()
        }

        @Test
        fun nextSucceedWhenNewRowInsertedAndCommittedByOtherTx() {
            // insert to unused row
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()
            tsByOtherTx.insert()
            tsByOtherTx.setInt("A", 1)
            tsByOtherTx.setString("B", "new value of 1")

            tsByOtherTx.close()
            otherTx.commit()

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThat(ts.next(), `is`(true))
            assertThat(ts.getInt("A"), `is`(1))
            assertThat(ts.getString("B"), `is`("new value of 1"))

            ts.close()
            tx.rollback()
        }
    }

    @Nested
    @DisplayName("Confirm that phantom read does not occur when inserting into a page at end of file")
    inner class Concurrent3 {
        private lateinit var db: SimpleDB

        @BeforeEach
        fun before() {
            // set block size to hold only 1 slot
            db = SimpleDB("dbdir/tableScanTest", 50, 8)
            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)

            // insert operation
            ts.beforeFirst()
            ts.insert()
            ts.setInt("A", 1)
            ts.setString("B", "value of 1")
            ts.close()
            tx.commit()
        }

        @Test
        fun insertFailWhenTableScanOpenedByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)

            // insert into new end of file page
            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThrows<LockAbortException> {
                ts.insert()
            }

            tsByOtherTx.close()
            otherTx.rollback()

            tx.rollback()
        }

        @Test
        fun insertSucceedWhenTableScanOpenedAndClosedByOtherTx() {
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.close()
            otherTx.rollback()

            // insert into new end of file page
            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            ts.insert()
            ts.setInt("A", 5)
            ts.setString("B", "value of 5")

            ts.beforeFirst()
            assertThat(ts.next(), `is`(true))
            assertThat(ts.getInt("A"), `is`(1))
            assertThat(ts.getString("B"), `is`("value of 1"))
            assertThat(ts.next(), `is`(true))
            assertThat(ts.getInt("A"), `is`(5))
            assertThat(ts.getString("B"), `is`("value of 5"))

            ts.close()
            tx.commit()
        }

        @Test
        fun openTableScanFailWhenNewRowInsertedByOtherTx() {
            // insert to unused row
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()
            tsByOtherTx.insert()
            tsByOtherTx.setInt("A", 5)
            tsByOtherTx.setString("B", "value of 5")

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            assertThrows<LockAbortException> {
                TableScan(tx, "T", layout)
            }

            tsByOtherTx.close()
            otherTx.commit()

            tx.rollback()
        }

        @Test
        fun nextSucceedWhenNewRowInsertedAndCommittedByOtherTx() {
            // insert to unused row
            val otherTx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val tsByOtherTx = TableScan(otherTx, "T", layout)
            tsByOtherTx.beforeFirst()
            tsByOtherTx.insert()
            tsByOtherTx.setInt("A", 5)
            tsByOtherTx.setString("B", "value of 5")

            tsByOtherTx.close()
            otherTx.commit()

            val tx = TransactionImpl(db.fileMgr(), db.logMgr(), db.bufferMgr())
            val ts = TableScan(tx, "T", layout)
            ts.beforeFirst()
            assertThat(ts.next(), `is`(true))
            assertThat(ts.getInt("A"), `is`(1))
            assertThat(ts.getString("B"), `is`("value of 1"))
            assertThat(ts.next(), `is`(true))
            assertThat(ts.getInt("A"), `is`(5))
            assertThat(ts.getString("B"), `is`("value of 5"))

            ts.close()
            tx.rollback()
        }
    }
}
