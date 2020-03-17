package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    int tableid;
    int pgno;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        tableid=tableId;
        pgno=pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tableid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgno;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     * 返回此页的哈希代码
     */
    public int hashCode() {
        // some code goes here
        return pgno;
    }

        /**
         * Compares one PageId to another.
         *
         * @param o The object to compare against (must be a PageId)
         * @return true if the objects are equal (e.g., page numbers and table
         *   ids are the same)
         *   一个页面id和另一个页面id比较是否相等 o必须是pageid而且页码和表id要相等
         */
    public boolean equals(Object o) {
        // some code goes here
        boolean x;
        x= o instanceof HeapPageId && pgno ==((HeapPageId) o).pgno && tableid==((HeapPageId) o).tableid;
        return x;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
