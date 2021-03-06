package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int pageNum;
    private final HashMap<PageId, Page> pid2page;
    private final LinkedList<PageId> lruList;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
		pid2page = new HashMap<>();
		lruList = new LinkedList<>();
		pageNum = numPages;
		lockManager = new LockManager();
	}
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

		if(perm == Permissions.READ_ONLY)
			lockManager.acquireLock(tid, pid, LockType.SHARED);
		else
			lockManager.acquireLock(tid, pid, LockType.EXCLUSIVE);

		synchronized (this) {
			if (pid2page.containsKey(pid)) {
				lruList.remove(pid);
				lruList.addLast(pid);
				return pid2page.get(pid);
			} else {
				DbFile tableFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
				Page newPage = tableFile.readPage(pid);
				if (lruList.size() >= pageNum) evictPage();
				lruList.addLast(pid);
				pid2page.put(pid, newPage);
				return newPage;
			}
		}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
		lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
		transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
		if(commit) flushPages(tid);
		else { // abort
			for(PageId pageId : lockManager.getExclusiveLockedPageIds(tid))
				discardPage(pageId);
		}

		lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirtyPages = file.insertTuple(tid, t);

		synchronized (this) {
			for (Page page : dirtyPages) {

				PageId pid = page.getId();
				if (pid2page.containsKey(pid))
					lruList.remove(pid);
				else if (lruList.size() >= pageNum) evictPage();
				lruList.addLast(pid);
				pid2page.put(pid, page);

				page.markDirty(true, tid);
			}
		}

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);

		synchronized (this) {
			for (Page page : dirtyPages) {

				PageId pid = page.getId();
				if (pid2page.containsKey(pid))
					lruList.remove(pid);
				else if (lruList.size() >= pageNum) evictPage();
				lruList.addLast(pid);
				pid2page.put(pid, page);

				page.markDirty(true, tid);
			}
		}

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
		for (PageId pid : pid2page.keySet())
			flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
		if(pid2page.containsKey(pid)){ // may have been evicted if not dirty
			Page pageToDiscard = pid2page.get(pid);
			pageToDiscard.markDirty(false, null);
			lruList.remove(pid);
			pid2page.remove(pid);
		}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
		if(pid2page.containsKey(pid)) {
			Page pageToFlush = pid2page.get(pid);
			DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
			table.writePage(pageToFlush);
			pageToFlush.markDirty(false, null);
		}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
		for(PageId pageId : lockManager.getExclusiveLockedPageIds(tid))
			flushPage(pageId);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

//		PageId pageIdToEvict = lruList.pollFirst();

		Iterator<PageId> iter = lruList.iterator();
		PageId pageIdToEvict = null;
		while(iter.hasNext()){
			PageId pageId = iter.next(); // concurrent modification exception may occur
			if(pid2page.get(pageId).isDirty() == null){ // not dirty
				pageIdToEvict = pageId;
				break;
			}
		}
		if(pageIdToEvict == null) throw new DbException("No clean page to evict.");

		try {
			flushPage(pageIdToEvict);
		}catch (IOException e){
			e.printStackTrace();
		}
		lruList.remove(pageIdToEvict);
		pid2page.remove(pageIdToEvict);
    }

}
