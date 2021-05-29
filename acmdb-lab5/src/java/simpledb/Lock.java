package simpledb;

import java.util.HashSet;
import java.util.Set;

/**
 * This class records information of locks on a specific page.
 */
public class Lock {

	public PageId pageId;
	public Set<TransactionId> sharedLockTidSet;
	public Set<TransactionId> exclusiveLockTidSet;

	public Lock(PageId pageId){
		this.pageId = pageId;
		this.sharedLockTidSet = new HashSet<>();
		this.exclusiveLockTidSet = new HashSet<>();
	}

}
