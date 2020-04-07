package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    int afield1;
    Predicate.Op aop;
    int afield2;
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        afield1=field1;
        aop=op;
        afield2=field2;
        // some code goes here
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        if(afield1<0 || afield1>t1.getTupleDesc().numFields() || afield2<0 || afield2>t2.getTupleDesc().numFields() )
            return false;
        return t1.getField(afield1).compare(aop,t2.getField(afield2));
    }
    
    public int getField1()
    {
        // some code goes here
        return afield1;
    }
    
    public int getField2()
    {
        // some code goes here
        return afield2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return aop;
    }
}
