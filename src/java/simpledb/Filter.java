package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate ap;
    OpIterator achild;
    public Filter(Predicate p, OpIterator child) {
        ap=p;
        achild=child;
        // some code goes here
    }

    public Predicate getPredicate() {
        // some code goes here
        return ap;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return achild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        achild.open();
        // some code goes here
    }

    public void close() {
        // some code goes here
        super.close();
        achild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

       achild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple now;
        now=null;
        achild.open();
        while(achild.hasNext())
        {
            Tuple tmple=achild.next();
            if(ap.filter(tmple))
            {
                now=tmple;
                break;
            }
        }
        return now;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator [] opit=new OpIterator[1];
        opit[0]=achild;
        return opit;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        achild=children[0];
    }

}
