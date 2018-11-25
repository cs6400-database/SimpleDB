package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CS6400
 * HOLD & MANAGE lock resource
 */
public class LockManager {
    private final Map<PageId, ReadWriteLock> lockMap;
    private final Map<TransactionId, Map<PageId, LockRecord>> lockHoldMap;
    private final Map<TransactionId, LockRecord> lockWaitMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
        lockHoldMap = new ConcurrentHashMap<>();
        lockWaitMap = new ConcurrentHashMap<>();
    }

    private void checkInitLock(PageId pid) {
        if (lockMap.get(pid) == null) {
            synchronized (lockMap) {
                if (lockMap.get(pid) == null)
                    lockMap.put(pid, new ReentrantReadWriteLock());
            }
        }
    }

    private void checkInitHoldTX(TransactionId tid) {
        if (lockHoldMap.get(tid) == null) {
            synchronized (lockHoldMap) {
                if (lockHoldMap.get(tid) == null)
                    lockHoldMap.put(tid, new ConcurrentHashMap<>());
            }
        }
    }

    public boolean checkHoldLock(TransactionId tid, PageId pid) {
        Map<PageId, LockRecord> detail = lockHoldMap.get(tid);
        if (detail != null) {
            return detail.get(pid) != null;
        }
        return false;
    }

    private boolean checkLock(TransactionId tid, PageId pid, Permissions permissions) {

        if (lockHoldMap.get(tid) != null) {
            LockRecord lockRecord = lockHoldMap.get(tid).get(pid);
            if (lockRecord != null && lockRecord.getPermissions() == permissions)
                return true;
        }
        return false;
    }

    public boolean grantReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        checkInitLock(pid);

        if (checkLock(tid, pid, Permissions.READ_ONLY))
            return true;
        //add waiting record
        LockRecord lockRecord = new LockRecord(tid, pid, Permissions.READ_ONLY);
        lockWaitMap.put(tid, lockRecord);
        ReadWriteLock readWriteLock = lockMap.get(pid);
        Lock rl = readWriteLock.readLock();
        checkInitHoldTX(tid);
        //start to wait for read lock
        if (!rl.tryLock()) {
            //
            if (deadLockExists(tid, pid)) {
                throw new TransactionAbortedException();
            }
            rl.lock();
        }
        //if success
        lockWaitMap.remove(tid);
        lockRecord.setLock(rl);

        lockHoldMap.get(tid).put(pid, lockRecord);
        return true;
    }

    public boolean grantWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        checkInitLock(pid);
        if (checkLock(tid, pid, Permissions.READ_WRITE))
            return true;
        //add waiting record
        LockRecord lockRecord = new LockRecord(tid, pid, Permissions.READ_WRITE);
        lockWaitMap.put(tid, lockRecord);
        ReadWriteLock readWriteLock = lockMap.get(pid);
        Lock wl = readWriteLock.writeLock();
        checkInitHoldTX(tid);
        //start to wait for write lock
        LockRecord readRecord = lockHoldMap.get(tid).get(pid);
        if (readRecord != null && readRecord.getPermissions() == Permissions.READ_ONLY) {
            //lock update
            readRecord.getLock().unlock();
            lockHoldMap.get(tid).remove(pid);
        }
        if (!wl.tryLock()) {
            //check
            if (deadLockExists(tid, pid)) {
                commitTX(tid);
                throw new TransactionAbortedException();
            }
            wl.lock();
        }
        //if success
        lockWaitMap.remove(tid);
        lockRecord.setLock(wl);
        lockHoldMap.get(tid).put(pid, lockRecord);
        return true;
    }

    public boolean releaseLock(TransactionId tid, PageId pid) {

        Map<PageId, LockRecord> detail = lockHoldMap.get(tid);
        if (detail != null) {
            LockRecord lockRecord = detail.remove(pid);
            if (lockRecord != null) {
                lockRecord.getLock().unlock();

                return true;
            }
        }
        return false;
    }

    public boolean commitTX(TransactionId tid) {
        Map<PageId, LockRecord> detail = lockHoldMap.remove(tid);
        if (detail != null) {
            for (Map.Entry<PageId, LockRecord> entry : detail.entrySet()) {

                if (entry.getValue().getThread() == Thread.currentThread())
                    entry.getValue().getLock().unlock();


            }
        }
        lockWaitMap.remove(tid);
        return true;
    }

    /**
     * check exists cycle possible?
     *
     * @param tid
     * @param pid
     * @return
     */
    private boolean deadLockExists(TransactionId tid, PageId pid) {
        Set<TransactionId> waitigSet = new HashSet<>();
        Set<PageId> searched = new HashSet<>();
        Queue<PageId> queue = new LinkedList<>();
        waitigSet.add(tid);
        queue.offer(pid);
        while (!queue.isEmpty()) {
            PageId pageId = queue.poll();
            searched.add(pageId);
            for (Map.Entry<TransactionId, Map<PageId, LockRecord>> entry : lockHoldMap.entrySet()) {
                if (entry.getValue().containsKey(pageId)) {
                    if (waitigSet.contains(entry.getKey()))
                        return true;
                    else {
                        waitigSet.add(entry.getKey());
                        LockRecord tos = lockWaitMap.get(entry.getKey());
                        if (tos != null && !searched.contains(tos.getPid()))
                            queue.offer(tos.getPid());
                    }
                }
            }
        }

        return false;
    }


}
