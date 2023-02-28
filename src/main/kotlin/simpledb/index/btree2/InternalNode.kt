package simpledb.index.btree2

import simpledb.query.Constant

class InternalNode {
    fun isBottom(): Boolean {
        TODO()
    }

    fun getEntrySize(): Int {
        TODO()
    }

    fun findEntry(key: Constant): Int {
        TODO()
    }

    fun fetchWithoutLatch(index: Int, key: Constant): InternalNode? {
        TODO()
    }

    fun latchShared() {
        TODO("Not yet implemented")
    }

    fun releaseLatch() {
        TODO("Not yet implemented")
    }

    fun isLatchOwner(): Boolean {
        TODO()
    }
}
