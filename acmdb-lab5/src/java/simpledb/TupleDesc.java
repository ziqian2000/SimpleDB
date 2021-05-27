package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private int numFields;
	private TDItem[] tdAr;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TDItem tdItem = (TDItem) o;
			boolean nameEquals = Objects.equals(fieldName, tdItem.fieldName);
			boolean typeEquals = fieldType.equals(tdItem.fieldType);
			return nameEquals && typeEquals;
		}

		@Override
		public int hashCode() {
			return Objects.hash(fieldType, fieldName);
		}
	}

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TDItem>{
    	private int pos = 0;

		@Override
		public boolean hasNext() {
			return tdAr.length > pos;
		}

		@Override
		public TDItem next() {
			return tdAr[this.pos++];
		}
	}

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
		this.numFields = typeAr.length;
		this.tdAr = new TDItem[this.numFields];
		for(int i = 0; i < this.numFields; i++){
			this.tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
		}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
		this(typeAr, new String[typeAr.length]);
    }

	private TupleDesc(TDItem[] tdItems) {
		this.tdAr = tdItems;
		this.numFields = tdItems.length;
	}

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= this.numFields) throw new NoSuchElementException();
        return this.tdAr[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
		if(i < 0 || i >= this.numFields) throw new NoSuchElementException();
		return this.tdAr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null) throw new NoSuchElementException();
        for(int i = 0; i < this.tdAr.length; i++){
        	if(name.equals(tdAr[i].fieldName))
        		return i;
		}
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int siz = 0;
		for (TDItem tdItem : tdAr) siz += tdItem.fieldType.getLen();
        return siz;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
		TDItem[] tdAr1 = td1.tdAr;
		TDItem[] tdAr2 = td2.tdAr;
		int length1 = tdAr1.length;
		int length2 = tdAr2.length;
		TDItem[] resultItems = new TDItem[length1 + length2];
		System.arraycopy(tdAr1, 0, resultItems, 0, length1);
		System.arraycopy(tdAr2, 0, resultItems, length1, length2);
		return new TupleDesc(resultItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TupleDesc tupleDesc = (TupleDesc) o;
		if (tupleDesc.numFields() != this.numFields())
			return false;
		for (int i = 0; i < numFields(); i++) {
			if (!tdAr[i].equals(tupleDesc.tdAr[i]))
				return false;
		}
		return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
		StringBuffer stringBuffer = new StringBuffer();
		for (TDItem tdItem : tdAr) {
			stringBuffer.append(tdItem.toString() + ", ");
		}
		return stringBuffer.toString();
    }
}
