package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

	private final int aggField;
	private final int groupByField;
	private final Type groupByType;

	private final Aggregator.Op aggOp;
	private TupleDesc resTupleDesc;

	private final HashMap<Field, Integer> resultMap;
	private int result;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
		this.groupByField = gbfield;
		this.groupByType = gbfieldtype;
		this.aggField = afield;
		this.aggOp = what;
		this.resultMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

		Type[] type;
		String[] name;
		if(this.groupByField == Aggregator.NO_GROUPING){
			type = new Type[1];
			name = new String[1];
			type[0] = Type.INT_TYPE;
			name[0] = tup.getTupleDesc().getFieldName(aggField);
		}
		else{
			type = new Type[2];
			name = new String[2];
			type[0] = groupByType;
			name[0] = tup.getTupleDesc().getFieldName(groupByField);
			type[1] = Type.INT_TYPE;
			name[1] = tup.getTupleDesc().getFieldName(aggField);
		}
		this.resTupleDesc = new TupleDesc(type, name);

		if(groupByField == NO_GROUPING){
			result += 1;
		}
		else{
			Field groupByVal = tup.getField(groupByField);
			if(!resultMap.containsKey(groupByVal)) resultMap.put(groupByVal, 0);
			resultMap.put(groupByVal, resultMap.get(groupByVal) + 1);
		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
		ArrayList<Tuple> tupleList = new ArrayList<>();

		if(groupByField == NO_GROUPING){
			Tuple tuple = new Tuple(resTupleDesc);
			tuple.setField(0, new IntField(result));
			tupleList.add(tuple);
		}
		else{
			for (Map.Entry<Field, Integer> entry : this.resultMap.entrySet())
			{
				Tuple tuple = new Tuple(this.resTupleDesc);
				tuple.setField(0, entry.getKey());
				tuple.setField(1, new IntField(entry.getValue()));
				tupleList.add(tuple);
			}
		}

		return new TupleIterator(resTupleDesc, tupleList);
    }

}
