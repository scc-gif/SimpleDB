package simpledb;

import java.util.List;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     *
     *            聚合构造函数
     *
     * *
     *
     * *@param gbfield参数
     *
     * *元组中group by字段的从0开始的索引，如果没有分组，则不进行分组
     *
     * *@param gbfieldtype参数
     *
     * *分组依据字段的类型（例如type.INT_type），如果没有分组，则为空
     *
     * *@param远方
     *
     * *元组中聚合字段的基于0的索引
     *
     * *@param什么
     *
     * *聚合运算符
     */
    int agbfield;
    Type agbfieldtype;
    int aafield;
    Op awhat;
    List<Tuple> tupleList,ans;
    List<Integer>  corrSum,corrNum,corrAns;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        agbfield=gbfield;
        agbfieldtype=gbfieldtype;
        aafield=afield;
        awhat=what;
        tupleList = new ArrayList<>();
        corrNum = new ArrayList<>();
        corrSum = new ArrayList<>();
        ans = new ArrayList<>();
        corrAns=new ArrayList<>();
        // some code goes here
    }

    public static Tuple getCorrTuple(Tuple tup, int i) {
        Type[] typeT = new Type[1];
        String[] nameT = new String[1];
        typeT[0] = tup.getTupleDesc().getFieldType(i);
        nameT[0] = tup.getTupleDesc().getFieldName(i);
        Tuple tupT = new Tuple(new TupleDesc(typeT, nameT));
        tupT.setField(0, tup.getField(i));
        return tupT;
    }
    public static Tuple getNewIntTuple(int i) {

        Type[] typeT = new Type[1];
        String[] nameT = new String[1];
        typeT[0] = Type.INT_TYPE;
        nameT[0] = "";
        Tuple ans = new Tuple(new TupleDesc(typeT, nameT));
        ans.setField(0, new IntField(i));
        return ans;
    }
    private void update(int i, int a) {
        if (corrSum.size() < corrNum.size()) {
            corrSum.add(a);
        } else if (awhat == Op.MIN) {
            corrSum.set(i, Math.min(corrSum.get(i), a));
        } else if (awhat == Op.MAX) {
            corrSum.set(i, Math.max(corrSum.get(i), a));
        } else if (awhat == Op.SUM) {
            corrSum.set(i, corrSum.get(i) + a);
        } else if (awhat == Op.AVG) {
            corrSum.set(i, corrSum.get(i) + a);
        }
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(agbfield == NO_GROUPING)
        {
            if(corrNum.size()==0)
                corrNum.add(1);
            else corrNum.set(0,corrNum.get(0)+1);
            update(0,tup.getField(aafield).hashCode());
        }
        else
            {
                int now=-1;
                for (int i=0;i<tupleList.size();i++)
                {
                    if(tupleList.get(i).getField(0).equals(getCorrTuple(tup,agbfield).getField(0)))
                    {
                        now =i;
                        corrNum.set(i,corrNum.get(i)+1);
                        break;
                    }
                }
                if(now == -1)
                {
                    corrNum.add(1);
                    now =corrNum.size()-1;
                    tupleList.add(getCorrTuple(tup,agbfield));
                }
                update(now,tup.getField(aafield).hashCode());
            }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        corrAns.clear();
        for (int i = 0; i <corrNum.size(); i++)
        {
            if(awhat==Op.AVG) {
                corrAns.add(i,corrSum.get(i)/corrNum.get(i));
            }
            else if (awhat == Op.COUNT) {
                corrAns.add(i, corrNum.get(i));
            }
            else {
                corrAns.add(i, corrSum.get(i));
            }
        }
        if (agbfield == NO_GROUPING) {
            ans.add(getNewIntTuple(corrAns.get(0)));//不进行分组
        }
        else {
            for (int i = 0; i < corrNum.size(); i++)
            {
                ans.add(Join.mergeTuple(tupleList.get(i), getNewIntTuple(corrAns.get(i))));
            }
        }
        return new TupleArrayIterator((ArrayList<Tuple>) ans);

}
}
