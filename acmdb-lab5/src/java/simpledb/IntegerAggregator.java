package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

	private final int aggField;
	private final int groupByField;
	private final Type groupByType;

	private final Aggregator.Op aggOp;
	private TupleDesc resTupleDesc;

	private final HashMap<Field, Integer> countMap;
	private final HashMap<Field, Integer> resultMap;
	private int count;
	private int result;

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
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
		this.groupByField = gbfield;
		this.groupByType = gbfieldtype;
		this.aggField = afield;
		this.aggOp = what;
		this.countMap = new HashMap<>();
		this.resultMap = new HashMap<>();
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

		Type aggType = tup.getField(aggField).getType();

		Type[] type;
		String[] name;
		if(this.groupByField == Aggregator.NO_GROUPING){
			type = new Type[1];
			name = new String[1];
			type[0] = this.aggOp == Aggregator.Op.COUNT ? Type.INT_TYPE : aggType;
			name[0] = tup.getTupleDesc().getFieldName(aggField);
		}
		else{
			type = new Type[2];
			name = new String[2];
			type[0] = groupByType;
			name[0] = tup.getTupleDesc().getFieldName(groupByField);
			type[1] = this.aggOp == Aggregator.Op.COUNT ? Type.INT_TYPE : aggType;
			name[1] = tup.getTupleDesc().getFieldName(aggField);
		}
		this.resTupleDesc = new TupleDesc(type, name);

		int aggVal = ((IntField)tup.getField(aggField)).getValue();
		if(groupByField == NO_GROUPING){
			count += 1;
			switch (aggOp) {
				case MIN:
					result = min(result, aggVal);
					break;
				case MAX:
					result = max(result, aggVal);
					break;
				case SUM:
				case AVG:
					result += aggVal;
					break;
				case COUNT:
					result = count;
					break;
			}
		}
		else{
			Field groupByVal = tup.getField(groupByField);

			if(!countMap.containsKey(groupByVal)) countMap.put(groupByVal, 0);
			countMap.put(groupByVal, countMap.get(groupByVal) + 1);

			if(!resultMap.containsKey(groupByVal)) resultMap.put(groupByVal, aggVal);
			else{
				int tmp = resultMap.get(groupByVal);
				switch (aggOp) {
					case MIN:
						resultMap.put(groupByVal, min(tmp, aggVal));
						break;
					case MAX:
						resultMap.put(groupByVal, max(tmp, aggVal));
						break;
					case SUM:
					case AVG:
						resultMap.put(groupByVal, tmp + aggVal);
						break;
					case COUNT:
						resultMap.put(groupByVal, countMap.get(groupByVal));
						break;
					default:
						assert false;
				}
			}

		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
		ArrayList<Tuple> tupleList = new ArrayList<>();

		if(groupByField == NO_GROUPING){
			Tuple tuple = new Tuple(resTupleDesc);
			if(aggOp == Op.AVG)
				tuple.setField(0, new IntField(result / count));
			else
				tuple.setField(0, new IntField(result));
			tupleList.add(tuple);
		}
		else{
			for (Map.Entry<Field, Integer> entry : this.resultMap.entrySet())
			{
				Tuple tuple = new Tuple(this.resTupleDesc);
				tuple.setField(0, entry.getKey());
				if (this.aggOp == Op.AVG)
					tuple.setField(1, new IntField(entry.getValue() / countMap.get(entry.getKey())));
				else
					tuple.setField(1, new IntField(entry.getValue()));
				tupleList.add(tuple);
			}
		}

		return new TupleIterator(resTupleDesc, tupleList);
    }

}
