package simpledb;

import java.util.*;

enum LockType{
	SHARED,
	EXCLUSIVE,
}

public class LockManager {

	public final int WAITING_TIME_LIMIT = 500; // waits for XXX ms
	public final int WAITING_INTERVAL = 50;
	public final Random randomGenerator = new Random();

	private final Map<PageId, Lock> pid2Lock;
	private final Map<TransactionId, Set<Lock>> tid2LockSet;

	public LockManager(){
		pid2Lock = new HashMap<>();
		tid2LockSet = new HashMap<>();
	}

	public synchronized void acquireLock(TransactionId tid, PageId pid, LockType lockType)
			throws TransactionAbortedException {

//		lockType = LockType.EXCLUSIVE; // XXX

		Lock lock = pid2Lock.computeIfAbsent(pid, Lock::new);
		Set<Lock> lockSet = tid2LockSet.computeIfAbsent(tid, key -> new HashSet<>());

		long endWaitingTime = System.currentTimeMillis() + WAITING_TIME_LIMIT + this.randomGenerator.nextInt(WAITING_TIME_LIMIT);

		if(lockType == LockType.EXCLUSIVE){
			while(true){
				if(lock.sharedLockTidSet.isEmpty() && lock.exclusiveLockTidSet.isEmpty()) break;
				if(lock.exclusiveLockTidSet.contains(tid)) break;
				if(lock.sharedLockTidSet.size() == 1 && lock.sharedLockTidSet.contains(tid)){
					lock.sharedLockTidSet.remove(tid);
					lock.toExclusiveTidSet.add(tid);
					break;
				}
				if(System.currentTimeMillis() > endWaitingTime) {
//					System.out.println(tid + " times out for exclusive locks on " + pid);
					throw new TransactionAbortedException();
				}
				try{
					wait(WAITING_INTERVAL);
//					System.out.println(tid + " is waiting for exclusive locks on " + pid);
				} catch (Exception e) {throw new TransactionAbortedException();}
			}
//			System.out.println(tid + " acquires the exclusive lock on " + pid);
			lock.exclusiveLockTidSet.add(tid);
		}

		else{
			while(true){
				if(lock.exclusiveLockTidSet.isEmpty()) break;
				if(lock.exclusiveLockTidSet.contains(tid)){
					lock.exclusiveLockTidSet.remove(tid);
					lock.toSharedTidSet.add(tid);
					break;
				}
				if(System.currentTimeMillis() > endWaitingTime) {
//					System.out.println(tid + " times out for shared locks on " + pid);
					throw new TransactionAbortedException();
				}
				try{
					wait(WAITING_INTERVAL);
//					System.out.println(tid + " is waiting for shared locks on " + pid);
				} catch (Exception e) {throw new TransactionAbortedException();}
			}
//			System.out.println(tid + " acquires the shared lock on " + pid);
			lock.sharedLockTidSet.add(tid);
		}

		lockSet.add(lock);
	}

	public synchronized void releaseLock(TransactionId tid, PageId pid){
		Lock lock = pid2Lock.get(pid);

		if(lock.exclusiveLockTidSet.contains(tid)) {
			lock.exclusiveLockTidSet.remove(tid);
//			System.out.println(tid + " releases the shared lock on " + pid);
		}
		else if(lock.sharedLockTidSet.contains(tid)) {
			lock.sharedLockTidSet.remove(tid);
//			System.out.println(tid + " releases the shared lock on " + pid);
		}
		else assert false;

//		if(lock.exclusiveLockTidSet.contains(tid)) lock.exclusiveLockTidSet.remove(tid);
//		else lock.sharedLockTidSet.remove(tid);

		tid2LockSet.get(tid).remove(lock);
		notifyAll();
	}

	public synchronized void releaseAllLocks(TransactionId tid){
		if(tid2LockSet.containsKey(tid)) {
			Set<Lock> lockSet = new HashSet<>(tid2LockSet.get(tid));
			for (Lock lock : lockSet) {
				releaseLock(tid, lock.pageId);
			}
		}
		notifyAll();
	}

	/**
	 * Get all pages that were ever exclusively locked by tid.
	 */
	public synchronized Set<PageId> getExclusiveLockedPageIds(TransactionId tid){
		Set<PageId> resPageId = new HashSet<>();
		if(tid2LockSet.containsKey(tid)) {
			for (Lock lock : tid2LockSet.get(tid)) {
				if (lock.exclusiveLockTidSet.contains(tid) || lock.toSharedTidSet.contains(tid))
					resPageId.add(lock.pageId);
			}
		}
		return resPageId;
	}

	public boolean holdsLock(TransactionId tid, PageId pid){
		Lock lock = pid2Lock.get(pid);
		return lock.exclusiveLockTidSet.contains(tid) || lock.sharedLockTidSet.contains(tid);
	}

}
