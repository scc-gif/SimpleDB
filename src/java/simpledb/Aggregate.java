package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    OpIterator achild;
    int aafield;
    int agfield;
    Aggregator.Op aaop;
    OpIterator tupleAggregation;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        achild=child;
        aafield=afield;
        agfield=gfield;
        aaop=aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return agfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if(agfield==Aggregator.NO_GROUPING)
	        return null;
        else
            return achild.getTupleDesc().getFieldName(agfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return aafield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return achild.getTupleDesc().getFieldName(aafield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aaop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop)
    {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        achild.open();
        Type tmp = achild.getTupleDesc().getFieldType(aafield);
        Aggregator myAgg;
        if (tmp == Type.INT_TYPE) {
            myAgg = new IntegerAggregator(
                    agfield,
                    agfield == -1 ? null : achild.getTupleDesc().getFieldType(agfield),
                    aafield,
                    aaop
            );
        } else {
            myAgg = new StringAggregator(
                    agfield,
                    agfield == -1 ? null : achild.getTupleDesc().getFieldType(agfield),
                    aafield,
                    aaop
            );
        }
        while (achild.hasNext()) {
            myAgg.mergeTupleIntoGroup(achild.next());
        }
        tupleAggregation = myAgg.iterator();
        tupleAggregation.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (tupleAggregation.hasNext())
            return tupleAggregation.next();
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        tupleAggregation.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return achild.getTupleDesc();
    }

    public void close() {
	// some code goes here
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        OpIterator[] tmp=new OpIterator[1];
        tmp[0]=achild;
        return tmp;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        achild=children[0];
    }

}
