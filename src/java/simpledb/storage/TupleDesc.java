package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }


        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (o instanceof TDItem) {
                TDItem other = (TDItem) o;
                if (Objects.equals(other.fieldName, this.fieldName) && Objects.equals(other.fieldType, this.fieldType)) {
                    return true;
                }
                return true;
            }
            return false;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    private List<TDItem> mList = new ArrayList<>();


    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return mList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            mList.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for (int i = 0; i < typeAr.length; i++) {
            mList.add(new TDItem(typeAr[i], "f" + i));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return mList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= numFields()) {
            // todo
            throw new NoSuchElementException("");
        }
        String fieldName = mList.get(i).fieldName;
        return fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= numFields()) {
            // todo
            throw new NoSuchElementException("");
        }
        Type type = mList.get(i).fieldType;
        return type;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int index = -1;
        for (int i = 0; i < mList.size(); i++) {
            if (name != null && name.equals(mList.get(i).fieldName)) {
                index = i;
            }
        }
        if (index == -1) {
            // todo
            throw new NoSuchElementException("");
        }
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = mList.stream().filter(item -> item != null).mapToInt((item) -> item.fieldType.getLen()).sum();
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

        int totalSize = td1.numFields() + td2.numFields();

        Type[] typeAr = new Type[totalSize];
        String[] fieldAr = new String[totalSize];

        Iterator<TDItem> iterator1 = td1.iterator();
        int index = 0;
        while (iterator1.hasNext()) {
            TDItem next = iterator1.next();
            typeAr[index] = next.fieldType;
            fieldAr[index] = next.fieldName;
            index++;
        }

        Iterator<TDItem> iterator2 = td2.iterator();
        while (iterator2.hasNext()) {
            TDItem next = iterator2.next();
            typeAr[index] = next.fieldType;
            fieldAr[index] = next.fieldName;
            index++;
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if (other.numFields() != numFields()) {
                return false;
            }
            Iterator<TDItem> iterator = iterator();
            Iterator<TDItem> otherIterator = other.iterator();
            while (iterator.hasNext()) {
                TDItem next = iterator.next();
                TDItem onext = otherIterator.next();
                if (next == null || onext == null) {
                    return false;
                }
                boolean equals = next.equals(onext);
                if (!equals) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    public int hashCode() {
        return mList.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        Iterator<TDItem> iterator = iterator();
        while (iterator.hasNext()) {
            TDItem next = iterator.next();
            sb.append("{fieldName:");
            sb.append(next.fieldName);
            sb.append(",fieldType:");
            sb.append(next.fieldType);
            sb.append("}");
            if (iterator.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");
        return "{list:" + sb + "}";
    }
}
