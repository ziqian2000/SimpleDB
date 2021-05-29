package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

enum LockType{
	SHARED,
	EXCLUSIVE,
}

public class LockManager {

	public final int WAITING_TIME_LIMIT = 300; // waits for at most 300 ms
//	public final int WAITING_INTERVAL = 50;

	private Map<PageId, Lock> pid2Lock;
	private Map<TransactionId, Set<Lock>> tid2LockSet;

	public LockManager(){
		pid2Lock = new HashMap<>();
		tid2LockSet = new HashMap<>();
	}

	public synchronized void acquireLock(TransactionId tid, PageId pid, LockType lockType)
			throws TransactionAbortedException {
		Lock lock = pid2Lock.computeIfAbsent(pid, Lock::new);
		Set<Lock> lockSet = tid2LockSet.computeIfAbsent(tid, key -> new HashSet<>());
		lockSet.add(lock);

		long endWaitingTime = System.currentTimeMillis() + WAITING_TIME_LIMIT;

		if(lockType == LockType.EXCLUSIVE){
			while(true){
				if(lock.sharedLockTidSet.isEmpty() && lock.exclusiveLockTidSet.isEmpty()) break;
				if(lock.exclusiveLockTidSet.contains(tid)) break;
				if(lock.exclusiveLockTidSet.isEmpty() && lock.sharedLockTidSet.contains(tid) && lock.sharedLockTidSet.size() == 1){
					lock.sharedLockTidSet.remove(tid);
					break;
				}
				if(System.currentTimeMillis() > endWaitingTime) throw new TransactionAbortedException();
				try{wait(WAITING_TIME_LIMIT);} catch (Exception e) {throw new TransactionAbortedException();}
			}
			lock.exclusiveLockTidSet.add(tid);
		}

		else{
			while(true){
				if(lock.exclusiveLockTidSet.isEmpty()) break;
				if(lock.exclusiveLockTidSet.contains(tid)){
					lock.exclusiveLockTidSet.remove(tid);
					break;
				}
				if(System.currentTimeMillis() > endWaitingTime) throw new TransactionAbortedException();
				try{wait(WAITING_TIME_LIMIT);} catch (Exception e) {throw new TransactionAbortedException();}
			}
			lock.sharedLockTidSet.add(tid);
		}
	}

	public synchronized void releaseLock(TransactionId tid, PageId pid){
		Lock lock = pid2Lock.get(pid);

		if(lock.exclusiveLockTidSet.contains(tid)) lock.exclusiveLockTidSet.remove(tid);
		else lock.sharedLockTidSet.remove(tid);

		tid2LockSet.get(tid).remove(lock);
		notifyAll();
	}

	public boolean holdsLock(TransactionId tid, PageId pid){
		Lock lock = pid2Lock.get(pid);
		return lock.exclusiveLockTidSet.contains(tid) || lock.sharedLockTidSet.contains(tid);
	}

}
