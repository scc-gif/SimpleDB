package simpledb;

import java.util.ArrayList;
import java.util.List;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     * *聚合构造函数
     *
     * *@param gbfield元组中group by字段的基于0的索引，如果没有分组，则不进行分组
     *
     * *@param gbfieldtype按字段分组的类型（例如type.INT_type），如果没有分组，则为空
     *
     * *@param afield元组中聚合字段的基于0的索引
     *
     * *@param要使用的聚合运算符--仅支持COUNT
     *
     * *@抛出IllegalArgumentException如果什么！=计数
     */

    int agbfield,aafield;
    Type agbfieldtype;
    Op awhat;
    List<Integer> numcount;
    List<Tuple> gbtuple,ans;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        agbfield=gbfield;
        agbfieldtype=gbfieldtype;
        aafield=afield;
        awhat=what;
        numcount=new ArrayList<>();
        gbtuple=new ArrayList<>();
        ans=new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(agbfield==NO_GROUPING)
        {
            if(numcount.size()==0)
                numcount.add(0);
            numcount.set(0,numcount.get(0)+1);
        }
        else
            {
                int now=-1;
                for (int i=0;i<gbtuple.size();i++)
                {
                    if(gbtuple.get(i).getField(0).equals(IntegerAggregator.getCorrTuple(tup,agbfield).getField(0)))
                    {
                        now=i;
                        break;
                    }
                }
                if(now==-1)
                {
                    gbtuple.add(IntegerAggregator.getCorrTuple(tup,agbfield));
                    now=gbtuple.size()-1;
                    numcount.add(0);
                }
                numcount.set(now,numcount.get(now)+1);
            }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (numcount.size() == 0)
        {
            return null;
        }
        if (agbfield == NO_GROUPING)
        {
            ans.add(IntegerAggregator.getNewIntTuple(numcount.get(0)));
        } else {
            for (int i = 0; i < numcount.size(); i++) {
                Tuple tmpT = IntegerAggregator.getNewIntTuple(numcount.get(i));
                ans.add(Join.mergeTuple(gbtuple.get(i), tmpT));
            }
        }
        return new TupleArrayIterator((ArrayList<Tuple>) ans);
    }

}
