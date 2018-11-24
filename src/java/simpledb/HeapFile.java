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
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.getPageSize() * pid.pageNumber();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException fe) {
            fe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int offset = BufferPool.getPageSize() * page.getId().pageNumber();
        byte[] data = page.getPageData();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(offset);
            raf.write(data);
            raf.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } 
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }

    public class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        int pageCounter;
        int tableId;
        int numPages;
        Page page;
        Iterator<Tuple> tuples;
        HeapPageId pid;
        
        /**
         * Constructor for Iterator
         * 
         * @param tid TransactionId of requesting transaction
         * @param tableId id of the HeapFile
         * @param numPages number of pages in file
         */
        public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
            this.tid = tid;
            this.pageCounter = 0;
            this.tableId = tableId;
            this.numPages = numPages;
        }
        
        /**
         * Gets tuples from file with given page number
         * 
         * @param pageNumber
         * @return Iterator &lt; Tuples &gt; Iterator for tuples in the page
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        private Iterator<Tuple> getTuples(int pageNumber) throws  DbException, TransactionAbortedException {
            pid = new HeapPageId(tableId, pageNumber);
            HeapPage heapPage = (HeapPage) Database.getBufferPool()
                    .getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        /**
         * Opens it iterator for iteration
         * 
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public void open() throws DbException, TransactionAbortedException {
            pageCounter = 0;
            tuples = getTuples(pageCounter);
        }

        /**
         * Checks if iterator has next tuple
         * 
         * @return boolean true if exists
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // If there are no tuples
            if(tuples == null)
                return false;
            // Check if tuple has next
            if(tuples.hasNext())
                return true;
            // Check if all pages are iterated
            if(pageCounter + 1 >= numPages)
                return false;
            // Else check if there is next page
            // If Page is exhausted get new page tuples
            while(pageCounter + 1 < numPages && !tuples.hasNext()){
                // Get tuples of next page
                pageCounter++;
                tuples = getTuples(pageCounter);
            }
            return this.hasNext();
        }

        /**
         * Get next tuple in this file 
         * 
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException 
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // If there are no tuples, throw exception
            if(tuples == null) 
                throw new NoSuchElementException();
            return tuples.next();
        }

        /**
         * Rewinds iterator to the start of file
         * 
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator
         */
        public void close() {
            tuples = null;
            pid = null;
        }
    }

}

