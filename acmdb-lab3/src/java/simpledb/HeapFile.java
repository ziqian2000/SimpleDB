package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File file;
	private TupleDesc tupleDesc;
	private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
		this.file = f;
		this.tupleDesc = td;
		this.numPages = (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
		Page pageToRead = null;
		try{
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek((long) pid.pageNumber() * BufferPool.getPageSize());
			byte[] data = new byte[BufferPool.getPageSize()];
			raf.read(data, 0, data.length);
			pageToRead = new HeapPage((HeapPageId) pid, data);
		}
		catch (IOException e){
			e.printStackTrace();
		}
		return pageToRead;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator{

    	private final TransactionId tid;
    	private Iterator<Tuple> tupleIterInPage;
    	private int pagePos;

    	public HeapFileIterator(TransactionId tid){
			this.tid = tid;
		}

		public Iterator<Tuple> getTupleIterInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		return page.iterator();
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			pagePos = 0;
			HeapPageId pid = new HeapPageId(getId(), pagePos);
			tupleIterInPage = getTupleIterInPage(pid);
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
    		if(tupleIterInPage == null) return false;
			if(tupleIterInPage.hasNext()) return true;
			while(pagePos + 1 < numPages()){
				tupleIterInPage = getTupleIterInPage(new HeapPageId(getId(), ++pagePos));
				if(tupleIterInPage.hasNext()){
					return true;
				}
			}
			return false;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if( !hasNext()) throw new NoSuchElementException();
			return tupleIterInPage.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			open();
		}

		@Override
		public void close() {
			tupleIterInPage = null;
		}
	}

}

