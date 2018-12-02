package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private Map<PageId, Page> pid2page;

    private final LockManager lockManager;
    private final Map<TransactionId, Set<PageId>> txUsedPage;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pid2page = new ConcurrentHashMap<>();
        lockManager = new LockManager();
        txUsedPage = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    } 

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {


        lockManager.grantWriteLock(tid, pid);
        if (!txUsedPage.containsKey(tid))
            txUsedPage.put(tid, new HashSet<>());
        txUsedPage.get(tid).add(pid);


        if (!pid2page.containsKey(pid)) {
            if (pid2page.size() == numPages) {
                evictPage();
            }
            pid2page.put(pid, Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid));

        }
        return pid2page.get(pid);
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
        // not necessary for proj1
        lockManager.releaseLock(tid, pid);
        txUsedPage.get(tid).remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.checkHoldLock(tid, p);
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
        // not necessary for proj1

        if (commit) {
            flushPages(tid);
        } else {
            discardPages(tid);
        }
        lockManager.commitTX(tid);
        txUsedPage.remove(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile table = (DbFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> affectedPages = table.insertTuple(tid, t);
        for (Page page : affectedPages) {
            page.markDirty(true, tid);
        }
        // not necessary for proj1
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile table = (DbFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> affectedPage = table.deleteTuple(tid, t);
        for (Page newPage : affectedPage) {
            newPage.markDirty(true, tid);
        }
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        for (PageId key : pid2page.keySet()) {
            flushPage(key);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
        pid2page.remove(pid);
        pid2page.put(pid, Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid));
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        Page page = pid2page.get(pid);
        int tableId = ((PageId)pid).getTableId();
        DbFile hf = (DbFile)Database.getCatalog().getDbFile(tableId);
        hf.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        if (txUsedPage.get(tid) != null) {
            for (PageId pid : txUsedPage.get(tid)) {
                if (pid2page.containsKey(pid))
                    flushPage(pid);
            }
        }
    }

    /**
     * Discard all pages of the specified transaction to disk.
     */
    public synchronized void discardPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        if (txUsedPage.get(tid) != null) {
            for (PageId pid : txUsedPage.get(tid)) {
                discardPage(pid);
            }
        }
    }
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        boolean evicted = false;
        for (Map.Entry<PageId, Page> entry : pid2page.entrySet()) {
            try {
                flushPage(entry.getKey());
                pid2page.remove(entry.getKey());
                evicted = true;
                break;
            } catch (IOException e) {
                throw new DbException("");
            }
        }
    }

}
