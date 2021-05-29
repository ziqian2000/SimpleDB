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
	public Set<TransactionId> toSharedTidSet;
	public Set<TransactionId> toExclusiveTidSet;

	public Lock(PageId pageId){
		this.pageId = pageId;
		this.sharedLockTidSet = new HashSet<>();
		this.exclusiveLockTidSet = new HashSet<>();
		this.toSharedTidSet = new HashSet<>();
		this.toExclusiveTidSet = new HashSet<>();
	}

}
