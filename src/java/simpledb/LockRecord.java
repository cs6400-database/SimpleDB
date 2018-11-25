package simpledb;

import java.util.concurrent.locks.Lock;

public class LockRecord {
    private final TransactionId tid;
    private final PageId pid;
    private final Permissions permissions;
    private final Thread thread;
    private Lock lock;

    public LockRecord(TransactionId tid, PageId pid, Permissions permissions) {
        this.tid = tid;
        this.pid = pid;
        this.permissions = permissions;
        thread = Thread.currentThread();
    }

    public Thread getThread() {
        return thread;
    }

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

    public TransactionId getTid() {
        return tid;
    }

    public PageId getPid() {
        return pid;
    }

    public Permissions getPermissions() {
        return permissions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LockRecord that = (LockRecord) o;

        if (tid != null ? !tid.equals(that.tid) : that.tid != null) return false;
        if (pid != null ? !pid.equals(that.pid) : that.pid != null) return false;
        return permissions != null ? permissions.equals(that.permissions) : that.permissions == null;
    }

    @Override
    public int hashCode() {
        int result = tid != null ? tid.hashCode() : 0;
        result = 31 * result + (pid != null ? pid.hashCode() : 0);
        result = 31 * result + (permissions != null ? permissions.hashCode() : 0);
        return result;
    }
}
