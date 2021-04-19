package simpledb;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private DbIterator childIter;
    private final int tableId;
    private final TupleDesc resTupleDesc;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
		this.transactionId = t;
		this.childIter = child;
		this.tableId = tableId;

		Type[] arr = new Type[1];
		arr[0] = Type.INT_TYPE;
		this.resTupleDesc = new TupleDesc(arr);

		called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return resTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
		super.open();
		childIter.open();
    }

    public void close() {
        // some code goes here
		super.close();
		childIter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
		childIter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(called) return null;
        int count = 0;
		ArrayList<Tuple> tmpArr = new ArrayList<>();
		while(childIter.hasNext()) tmpArr.add(childIter.next());
		for(Tuple tupleToInsert : tmpArr){
        	try {
				Database.getBufferPool().insertTuple(transactionId, tableId, tupleToInsert);
				count++;
			} catch (IOException ignored) {}
		}
        Tuple tuple = new Tuple(resTupleDesc);
        tuple.setField(0, new IntField(count));
        called = true;
        return tuple;
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
