package simpledb.tx.concurrency.rowlock

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasKey
import org.hamcrest.Matchers.`is`
import org.junit.jupiter.api.Test
import simpledb.file.BlockId
import kotlin.concurrent.thread

class BlockLatchTest {
    @Test
    fun rLatchSucceed() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.rLatch(targetBlk)
        val latches = blockLatch.latches()
        assertThat(latches, hasKey(targetBlk))
    }

    @Test
    fun rLatchSucceedTwice() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.rLatch(targetBlk)
        blockLatch.rLatch(targetBlk)
        val latches = blockLatch.latches()
        assertThat(latches, hasKey(targetBlk))
    }

    @Test
    fun wLatchSucceed() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.wLatch(targetBlk)
        val latches = blockLatch.latches()
        assertThat(latches, hasKey(targetBlk))
    }

    @Test
    fun rLatchFailedWhenWLatchAcquired() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.wLatch(targetBlk)
        val t1 = thread {
            blockLatch.rLatch(targetBlk)
        }
        t1.join(100)
        assertThat(t1.isAlive, `is`(true))
    }

    @Test
    fun rLatchSucceedWhenWLatchAcquiredAndReleased() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.wLatch(targetBlk)
        val t1 = thread {
            blockLatch.rLatch(targetBlk)
        }
        t1.join(100)
        assertThat(t1.isAlive, `is`(true))

        blockLatch.wUnlatch(targetBlk)
        t1.join(100)
        assertThat(t1.isAlive, `is`(false))
    }

    @Test
    fun wLatchFailedWhenRLatchAcquired() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.rLatch(targetBlk)
        val t1 = thread {
            blockLatch.wLatch(targetBlk)
        }
        t1.join(100)
        assertThat(t1.isAlive, `is`(true))
    }

    @Test
    fun wLatchSucceedWhenRLatchAcquiredAndReleased() {
        val targetBlk = BlockId("targetFile", 1)
        val blockLatch = BlockLatch()
        blockLatch.rLatch(targetBlk)
        val t1 = thread {
            blockLatch.wLatch(targetBlk)
        }
        t1.join(100)
        assertThat(t1.isAlive, `is`(true))

        blockLatch.rUnlatch(targetBlk)
        t1.join(100)
        assertThat(t1.isAlive, `is`(false))
    }

}
