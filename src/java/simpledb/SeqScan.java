package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    TransactionId atid;
    int atableid;
    String atableAlias;
    DbFileIterator tpiterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        atid=tid;
        atableid=tableid;
        atableAlias=tableAlias;
        HeapFile tmp = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        tpiterator = tmp.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return atableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        atableid=tableid;
        atableAlias=tableAlias;
        HeapFile tmp = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        tpiterator = tmp.iterator(atid);

        // some code goes here
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        tpiterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    /**

     *从基础HeapFile返回具有字段名的TupleDesc，

     *以构造函数中的tableAlias字符串作为前缀。这个前缀

     *将包含字段的表与

     *名字。别名和名称应该用“.”字符分隔

     *（例如，“alias.fieldName”）。

     *

     *@从底层HeapFile返回带有字段名的TupleDesc，

     *以构造函数中的tableAlias字符串作为前缀。

     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tmp=Database.getCatalog().getTupleDesc(atableid);
        Type[] t=new Type[tmp.numFields()];
        String[] name=new String[tmp.numFields()];
        for (int i = 0; i <tmp.numFields() ; i++)
        {
            t[i]=tmp.getFieldType(i);
            name[i]=atableAlias+"."+tmp.getFieldName(i);
        }
        return new TupleDesc(t,name);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return tpiterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return tpiterator.next();
    }

    public void close() {
        // some code goes here
        tpiterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        tpiterator.rewind();
    }
}
