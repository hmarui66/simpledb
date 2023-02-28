package simpledb.index.btree2

import simpledb.query.Constant

class BTree {

    // 検索してリーフの一つ上の中間ノードを返す
    // 返されるノードの共有 latch はかけられたままとなる
    fun search(key: Constant): BottomInternalNode? {
        // ルートを取得
        val root = getRoot() ?: return null

        var success = false
        var index: Int
        var parent = root
        var child: InternalNode? = null

        try {
            do {
                check(parent.getEntrySize() != 0) {
                    "parent with 0 child"
                }

                index = parent.findEntry(key)

                child = parent.fetchWithoutLatch(index, key)

                // 無限ループにならないか？
                if (child == null) {
                    parent = getRoot() ?: return null

                    continue
                }

                // child を latch
                // BottomInternalNode は常に排他 latch がされている前提
                child.latchShared()

                parent.releaseLatch()
                parent = child

                child = null
            } while (!parent.isBottom())

            success = true
            return parent as BottomInternalNode
        } finally {
            if (!success) {
                if (child != null && child.isLatchOwner()) {
                    child.releaseLatch()
                }

                if (parent != child && parent.isLatchOwner()) {
                    parent.releaseLatch()
                }
            }
        }
    }

    private fun getRoot(): InternalNode? {
        TODO()
    }

    fun insert(leaf: Leaf) {
        TODO()
    }
}
