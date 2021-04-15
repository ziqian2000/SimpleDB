package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

	private final TransactionId transactionId;
	private DbIterator childIter;
	private final TupleDesc resTupleDesc;
	private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
		this.transactionId = t;
		this.childIter = child;

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
		if(called) return null;
		int count = 0;
		ArrayList<Tuple> tmpArr = new ArrayList<>();
		while(childIter.hasNext()) tmpArr.add(childIter.next());
		for(Tuple tupleToDelete : tmpArr){
			try {
				Database.getBufferPool().deleteTuple(transactionId, tupleToDelete);
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
