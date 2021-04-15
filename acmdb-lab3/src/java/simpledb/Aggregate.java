package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator childIter;

    private int aggField;
    private int groupByField;
    private Type aggType;
    private Type groupByType;

    private Aggregator.Op aggOp;
    private TupleDesc resTupleDesc;
    private Aggregator aggregator;
    private DbIterator aggIter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.childIter = child;
		this.aggField = afield;
		this.groupByField = gfield;
		this.aggOp = aop;

		this.aggType = this.childIter.getTupleDesc().getFieldType(this.aggField);

		Type[] type;
		String[] name;
		if(this.groupByField == Aggregator.NO_GROUPING){
			this.groupByType = null;
			type = new Type[1];
			name = new String[1];
			type[0] = this.aggOp == Aggregator.Op.COUNT ? Type.INT_TYPE : aggType;
			name[0] = this.childIter.getTupleDesc().getFieldName(aggField);
		}
		else{
			this.groupByType = this.childIter.getTupleDesc().getFieldType(this.groupByField);
			type = new Type[2];
			name = new String[2];
			type[0] = groupByType;
			name[0] = this.childIter.getTupleDesc().getFieldName(groupByField);
			type[1] = this.aggOp == Aggregator.Op.COUNT ? Type.INT_TYPE : aggType;
			name[1] = this.childIter.getTupleDesc().getFieldName(aggField);
		}
		this.resTupleDesc = new TupleDesc(type, name);
	}

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		// some code goes here
		return groupByField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		// some code goes here
		return this.groupByField == Aggregator.NO_GROUPING ? null : this.resTupleDesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		// some code goes here
		return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return this.groupByField == Aggregator.NO_GROUPING
				? this.resTupleDesc.getFieldName(0)
				: this.resTupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		// some code goes here
		super.open();
		childIter.open();
		if(aggType == Type.INT_TYPE)
			aggregator = new IntegerAggregator(groupByField, groupByType, aggField, aggOp);
		else
			aggregator = new StringAggregator(groupByField, groupByType, aggField, aggOp);
		while (childIter.hasNext()) aggregator.mergeTupleIntoGroup(childIter.next());
		aggIter = aggregator.iterator();
		aggIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		return aggIter.hasNext() ? aggIter.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		aggIter.rewind();
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
		return resTupleDesc;
    }

    public void close() {
		// some code goes here
		super.close();
		aggIter.close();
    }

    @Override
    public DbIterator[] getChildren() {
		// some code goes here
		DbIterator[] arr = new DbIterator[1];
		arr[0] = childIter;
		return arr;
    }

    @Override
    public void setChildren(DbIterator[] children) {
		// some code goes here
		childIter = children[0];
    }
    
}
