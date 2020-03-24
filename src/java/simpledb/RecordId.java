package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    PageId apid;
    int atupleno;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    PageId pidx;
    int tuplenox;
    public RecordId(PageId pid, int tupleno) {
        apid=pid;
        atupleno=tupleno;
        // some code goes here
        pidx=pid;
        tuplenox=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tuplenox;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pidx;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     * 如果两个RecordId对象表示同一个
     *
     * *元组。
     *
     * *
     *
     * *@return True如果this和o代表同一元组
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        boolean x;
        x= o instanceof RecordId &&  this.getPageId().equals(((RecordId) o).getPageId()) && this.getTupleNumber()==((RecordId) o).getTupleNumber();
        return x;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        int num=2*pidx.hashCode()+1;
        return num;
    }

}
