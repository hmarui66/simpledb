package simpledb.index.btree2

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import simpledb.query.Constant

class BTreeTest {
    @Test
    fun retrieveLeaf() {
        val key = Constant(1)
        val tree = BTree()

        val bin = tree.search(key)
        checkNotNull(bin)
        try {
            val index = bin.findEntry(key)
            assertNotEquals(index, -1)
            val leaf = bin.getTarget(index)
            assertNotNull(leaf)
        } finally {
            bin.releaseLatch()
        }
    }
}
