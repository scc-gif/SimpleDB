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

    File afile;
    TupleDesc atd;
    int nowpage;
    /**1111111
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        afile=f;
        atd=td;
        nowpage=-1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return afile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return afile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return atd;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            byte[] data = new byte[BufferPool.getPageSize()];
            FileInputStream ss = new FileInputStream(afile);
            ss.read(data);
            HeapPage l = new HeapPage((HeapPageId) pid, data);
            return l;
        } catch (IOException o) {

        } finally { }
        return null;
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
        return (int) (afile.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if (t == null) {
            throw new IOException();
        }

        for (int i = 0; i < numPages(); i++) {
            HeapPage tmp = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), null));
            if (tmp.getNumEmptySlots() != 0) {
                tmp.insertTuple(t);
                tmp.markDirty(true, tid);
                ArrayList<Page> output = new ArrayList<>();
                output.add(tmp);
                return output;
            }
        }
        HeapPage p = new HeapPage(new HeapPageId(getId(), numPages()),
                HeapPage.createEmptyPageData());
        p.insertTuple(t);
        p.markDirty(false, tid);
        byte[] bt=p.getPageData();
        FileOutputStream bw = new FileOutputStream(afile, true);
        bw.write(bt);
        bw.close();
        ArrayList<Page> output = new ArrayList<>();
        output.add(p);
        return output;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        if (t == null
                || t.getRecordId().getPageId().getTableId() != getId()
                || t.getRecordId().getPageId().getPageNumber() < 0
                || t.getRecordId().getPageId().getPageNumber() >= numPages())
        throw new DbException("he tuple cannot be deleted or is not a member of the file");
        HeapPage tmp = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        tmp.deleteTuple(t);
        tmp.markDirty(true, tid);
        ArrayList<Page> output = new ArrayList<>();
        output.add(tmp);
        return output;
        // not necessary for lab1
    }

     class TupleListIterator implements DbFileIterator {
        private List<Tuple> tuples;
        private boolean isOpen;
        Iterator<Tuple> now;

        public TupleListIterator(PageId pid) {
            tuples = getNextVailPage();
            isOpen = false;
            now = null;
            nowpage=0;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            now = tuples.iterator();
        }
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen)
                return false;
            if (now.hasNext())
                return true;
            if(nowpage>=numPages()-1)
                return false;
            nowpage++;
            return getNextVailPage().size() != 0;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen || now == null) {
                throw new NoSuchElementException();
            }
            if (!now.hasNext()) {
                tuples = getNextVailPage();
                now = tuples.iterator();
            }
            return now.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("not support");
            }
            nowpage=0;
            tuples =getNextVailPage();
            now = tuples.iterator();
        }

        @Override
        public void close() {
            isOpen = false;
        }

         private ArrayList<Tuple> getNextVailPage() {
             for (int i = nowpage; i < numPages(); i++) {
                 ArrayList<Tuple> tmp = loadPageToList(new HeapPageId(getId(), i));
                 if (tmp.size() != 0)
                     return tmp;
                 nowpage++;
             }
             return new ArrayList<>();
         }
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        nowpage = 0;
        return new TupleListIterator(new HeapPageId(getId(), nowpage));
    }

    private ArrayList<Tuple> loadPageToList(PageId pid) {
        try {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
            Iterator<Tuple> it = p.iterator();
            ArrayList<Tuple> t = new ArrayList<>();
            while (it.hasNext()) {
                t.add(it.next());
            }
            return t;
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

