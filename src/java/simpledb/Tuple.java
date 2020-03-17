package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    TupleDesc tddesc;
    RecordId tdid;
    List<Field> tdcat;

    private static final long serialVersionUID = 1L;

        /**
         * Create a new tuple with the specified schema (type).
         *
         * @param td
         *            the schema of this tuple. It must be a valid TupleDesc
         *            instance with at least one field.
         */
    public Tuple(TupleDesc td) {
        // some code goes here
        tdid=null;
        tdcat=new ArrayList<>();
        for (int i = 0; i < td.numFields(); i++)
        {
            tdcat.add(null);
        }
        tddesc=td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tddesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return tdid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        tdid=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        tdcat.set(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return tdcat.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String astring = new String();
        for (int i = 0; i < tdcat.size(); i++)
        {
            if (i != 0)
            {
                astring += " ";
            }
            if (tdcat.get(i) instanceof StringField)
            {
                astring += ((StringField) tdcat.get(i)).getValue();
            }
            else {
                astring += String.format("%d", ((IntField) tdcat.get(i)).getValue());
            }
        }
        return astring;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return tdcat.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tddesc=td;
    }
}
