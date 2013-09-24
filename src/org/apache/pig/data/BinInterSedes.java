/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.impl.util.ObjectSerializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A class to handle reading and writing of intermediate results of data types. The serialization format used by this
 * class more efficient than what was used in DataReaderWriter . The format used by the functions in this class is
 * subject to change, so it should be used ONLY to store intermediate results within a pig query.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BinInterSedes implements InterSedes {

    private static final int ONE_MINUTE = 60000;

    public static final byte BOOLEAN_TRUE = 0;
    public static final byte BOOLEAN_FALSE = 1;

    public static final byte BYTE = 2;

    public static final byte INTEGER = 3;
    // since boolean is not supported yet(v0.7) as external type, lot of people use int instead and some data with old
    // schema is likely stay for some time. so optimizing for that case as well
    public static final byte INTEGER_0 = 4;
    public static final byte INTEGER_1 = 5;
    public static final byte INTEGER_INSHORT = 6;
    public static final byte INTEGER_INBYTE = 7;

    public static final byte LONG = 8;
    public static final byte FLOAT = 9;
    public static final byte DOUBLE = 10;

    public static final byte BYTEARRAY = 11;
    public static final byte SMALLBYTEARRAY = 12;
    public static final byte TINYBYTEARRAY = 13;

    public static final byte CHARARRAY = 14;
    public static final byte SMALLCHARARRAY = 15;

    public static final byte MAP = 16;
    public static final byte SMALLMAP = 17;
    public static final byte TINYMAP = 18;

    public static final byte TUPLE = 19;
    public static final byte SMALLTUPLE = 20;
    public static final byte TINYTUPLE = 21;

    public static final byte BAG = 22;
    public static final byte SMALLBAG = 23;
    public static final byte TINYBAG = 24;

    public static final byte GENERIC_WRITABLECOMPARABLE = 25;
    public static final byte INTERNALMAP = 26;

    public static final byte NULL = 27;

    public static final byte SCHEMA_TUPLE_BYTE_INDEX = 28;
    public static final byte SCHEMA_TUPLE_SHORT_INDEX = 29;
    public static final byte SCHEMA_TUPLE = 30;

    public static final byte LONG_INBYTE = 31;
    public static final byte LONG_INSHORT = 32;
    public static final byte LONG_ININT = 33;
    public static final byte LONG_0 = 34;
    public static final byte LONG_1 = 35;

    public static final byte TUPLE_0 = 36;
    public static final byte TUPLE_1 = 37;
    public static final byte TUPLE_2 = 38;
    public static final byte TUPLE_3 = 39;
    public static final byte TUPLE_4 = 40;
    public static final byte TUPLE_5 = 41;
    public static final byte TUPLE_6 = 42;
    public static final byte TUPLE_7 = 43;
    public static final byte TUPLE_8 = 44;
    public static final byte TUPLE_9 = 45;

    public static final byte BIGINTEGER = 46;
    public static final byte BIGDECIMAL = 47;

    public static final byte DATETIME = 48;

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();
    public static final int UNSIGNED_SHORT_MAX = 65535;
    public static final int UNSIGNED_BYTE_MAX = 255;
    public static final String UTF8 = "UTF-8";

    public Tuple readTuple(DataInput in, byte type) throws IOException {
        switch (type) {
        case TUPLE_0:
        case TUPLE_1:
        case TUPLE_2:
        case TUPLE_3:
        case TUPLE_4:
        case TUPLE_5:
        case TUPLE_6:
        case TUPLE_7:
        case TUPLE_8:
        case TUPLE_9:
        case TUPLE:
        case TINYTUPLE:
        case SMALLTUPLE:
            return SedesHelper.readGenericTuple(in, type);
        case SCHEMA_TUPLE_BYTE_INDEX:
        case SCHEMA_TUPLE_SHORT_INDEX:
        case SCHEMA_TUPLE:
            return readSchemaTuple(in, type);
        default:
            throw new ExecException("Unknown Tuple type found in stream: " + type);
        }
        }

    private Tuple readSchemaTuple(DataInput in, byte type) throws IOException {
        int id;
        switch (type) {
        case (SCHEMA_TUPLE_BYTE_INDEX): id = in.readUnsignedByte(); break;
        case (SCHEMA_TUPLE_SHORT_INDEX): id = in.readUnsignedShort(); break;
        case (SCHEMA_TUPLE): id = in.readInt(); break;
        default: throw new RuntimeException("Invalid type given to readSchemaTuple: " + type);
    }

        Tuple st = SchemaTupleFactory.getInstance(id).newTuple();
        st.readFields(in);

        return st;
    }

    public int getTupleSize(DataInput in, byte type) throws IOException {
        int sz;
        switch (type) {
        case TUPLE_0:
            return 0;
        case TUPLE_1:
            return 1;
        case TUPLE_2:
            return 2;
        case TUPLE_3:
            return 3;
        case TUPLE_4:
            return 4;
        case TUPLE_5:
            return 5;
        case TUPLE_6:
            return 6;
        case TUPLE_7:
            return 7;
        case TUPLE_8:
            return 8;
        case TUPLE_9:
            return 9;
        case TINYTUPLE:
            sz = in.readUnsignedByte();
            break;
        case SMALLTUPLE:
            sz = in.readUnsignedShort();
            break;
        case TUPLE:
            sz = in.readInt();
            break;
        default: {
            int errCode = 2112;
            String msg = "Unexpected datatype " + type + " while reading tuple" + "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        }
        // if sz == 0, we construct an "empty" tuple - presumably the writer wrote an empty tuple!
        if (sz < 0) {
            throw new IOException("Invalid size " + sz + " for a tuple");
        }
        return sz;
    }

    private DataBag readBag(DataInput in, byte type) throws IOException {
        DataBag bag = mBagFactory.newDefaultBag();
        long size;
        // determine size of bag
        switch (type) {
        case TINYBAG:
            size = in.readUnsignedByte();
            break;
        case SMALLBAG:
            size = in.readUnsignedShort();
            break;
        case BAG:
            size = in.readLong();
            break;
        default:
            int errCode = 2219;
            String msg = "Unexpected data while reading bag " + "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        for (long i = 0; i < size; i++) {
            try {
                Object o = readDatum(in);
                bag.add((Tuple) o);
            } catch (ExecException ee) {
                throw ee;
            }
        }
        return bag;
    }

    private Map<String, Object> readMap(DataInput in, byte type) throws IOException {
        int size;
        switch (type) {
        case TINYMAP:
            size = in.readUnsignedByte();
            break;
        case SMALLMAP:
            size = in.readUnsignedShort();
            break;
        case MAP:
            size = in.readInt();
            break;
        default: {
            int errCode = 2220;
            String msg = "Unexpected data while reading map" + "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        }
        Map<String, Object> m = new HashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            String key = (String) readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;
    }

    private InternalMap readInternalMap(DataInput in) throws IOException {
        int size = in.readInt();
        InternalMap m = new InternalMap(size);
        for (int i = 0; i < size; i++) {
            Object key = readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;
    }

    private WritableComparable readWritable(DataInput in) throws IOException {
        String className = (String) readDatum(in);
        // create the writeable class . It needs to have a default constructor
        Class<?> objClass = null;
        try {
            objClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not find class " + className + ", while attempting to de-serialize it ", e);
        }
        WritableComparable writable = null;
        try {
            writable = (WritableComparable) objClass.newInstance();
        } catch (Exception e) {
            String msg = "Could create instance of class " + className
                    + ", while attempting to de-serialize it. (no default constructor ?)";
            throw new IOException(msg, e);
        }

        // read the fields of the object from DataInput
        writable.readFields(in);
        return writable;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.data.InterSedes#readDatum(java.io.DataInput)
     */
    @Override
    public Object readDatum(DataInput in) throws IOException, ExecException {
        // Read the data type
        byte b = in.readByte();
        return readDatum(in, b);
    }

    private static Object readBytes(DataInput in, int size) throws IOException {
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new DataByteArray(ba);
    }

    /**
     * Expects binInterSedes data types (NOT DataType types!)
     * <p>
     *
     * @see org.apache.pig.data.InterSedes#readDatum(java.io.DataInput, byte)
     */
    @Override
    public Object readDatum(DataInput in, byte type) throws IOException, ExecException {
        switch (type) {
        case TUPLE_0:
        case TUPLE_1:
        case TUPLE_2:
        case TUPLE_3:
        case TUPLE_4:
        case TUPLE_5:
        case TUPLE_6:
        case TUPLE_7:
        case TUPLE_8:
        case TUPLE_9:
        case TUPLE:
        case TINYTUPLE:
        case SMALLTUPLE:
            return SedesHelper.readGenericTuple(in, type);

        case BAG:
        case TINYBAG:
        case SMALLBAG:
            return readBag(in, type);

        case MAP:
        case TINYMAP:
        case SMALLMAP:
            return readMap(in, type);

        case INTERNALMAP:
            return readInternalMap(in);

        case INTEGER_0:
            return Integer.valueOf(0);
        case INTEGER_1:
            return Integer.valueOf(1);
        case INTEGER_INBYTE:
            return Integer.valueOf(in.readByte());
        case INTEGER_INSHORT:
            return Integer.valueOf(in.readShort());
        case INTEGER:
            return Integer.valueOf(in.readInt());

        case LONG_0:
            return Long.valueOf(0);
        case LONG_1:
            return Long.valueOf(1);
        case LONG_INBYTE:
            return Long.valueOf(in.readByte());
        case LONG_INSHORT:
            return Long.valueOf(in.readShort());
        case LONG_ININT:
            return Long.valueOf(in.readInt());
        case LONG:
            return Long.valueOf(in.readLong());

        case DATETIME:
            return new DateTime(in.readLong(), DateTimeZone.forOffsetMillis(in.readShort() * ONE_MINUTE));

        case FLOAT:
            return Float.valueOf(in.readFloat());

        case DOUBLE:
            return Double.valueOf(in.readDouble());

        case BIGINTEGER:
            return readBigInteger(in);

        case BIGDECIMAL:
            return readBigDecimal(in);

        case BOOLEAN_TRUE:
            return Boolean.valueOf(true);

        case BOOLEAN_FALSE:
            return Boolean.valueOf(false);

        case BYTE:
            return Byte.valueOf(in.readByte());

        case TINYBYTEARRAY:
        case SMALLBYTEARRAY:
        case BYTEARRAY:
            return new DataByteArray(SedesHelper.readBytes(in, type));

        case CHARARRAY:
        case SMALLCHARARRAY:
            return SedesHelper.readChararray(in, type);

        case GENERIC_WRITABLECOMPARABLE:
            return readWritable(in);

        case SCHEMA_TUPLE_BYTE_INDEX:
        case SCHEMA_TUPLE_SHORT_INDEX:
        case SCHEMA_TUPLE:
            return readSchemaTuple(in, type);

        case NULL:
            return null;

        default:
            throw new RuntimeException("Unexpected data type " + type + " found in stream.");
        }
    }

    private Object readBigDecimal(DataInput in) throws IOException {
        return  new BigDecimal((String)readDatum(in));
    }

    private Object readBigInteger(DataInput in) throws IOException {
        return new BigInteger((String)readDatum(in));
    }

    private void writeBigInteger(DataOutput out, BigInteger bi) throws IOException {
        writeDatum(out, bi.toString());
    }

    private void writeBigDecimal(DataOutput out, BigDecimal bd) throws IOException {
        writeDatum(out, bd.toString());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.data.InterSedes#writeDatum(java.io.DataOutput, java.lang.Object)
     */
    @Override
    public void writeDatum(DataOutput out, Object val) throws IOException {
        // Read the data type
        byte type = DataType.findType(val);
        writeDatum(out, val, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeDatum(DataOutput out, Object val, byte type) throws IOException {
        switch (type) {
        case DataType.TUPLE:
            writeTuple(out, (Tuple) val);
            break;

        case DataType.BAG:
            writeBag(out, (DataBag) val);
            break;

        case DataType.MAP: {
            writeMap(out, (Map<String, Object>) val);
            break;
        }

        case DataType.INTERNALMAP: {
            out.writeByte(INTERNALMAP);
            Map<Object, Object> m = (Map<Object, Object>) val;
            out.writeInt(m.size());
            Iterator<Map.Entry<Object, Object>> i = m.entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry<Object, Object> entry = i.next();
                writeDatum(out, entry.getKey());
                writeDatum(out, entry.getValue());
            }
            break;
        }

        case DataType.INTEGER:
            int i = (Integer) val;
            if (i == 0) {
                out.writeByte(INTEGER_0);
            } else if (i == 1) {
                out.writeByte(INTEGER_1);
            } else if (Byte.MIN_VALUE <= i && i <= Byte.MAX_VALUE) {
                out.writeByte(INTEGER_INBYTE);
                out.writeByte(i);
            } else if (Short.MIN_VALUE <= i && i <= Short.MAX_VALUE) {
                out.writeByte(INTEGER_INSHORT);
                out.writeShort(i);
            } else {
                out.writeByte(INTEGER);
                out.writeInt(i);
            }
            break;

        case DataType.LONG:
            long lng = (Long) val;
            if (lng == 0) {
                out.writeByte(LONG_0);
            } else if (lng == 1) {
                out.writeByte(LONG_1);
            } else if (Byte.MIN_VALUE <= lng && lng <= Byte.MAX_VALUE) {
                out.writeByte(LONG_INBYTE);
                out.writeByte((int)lng);
            } else if (Short.MIN_VALUE <= lng && lng <= Short.MAX_VALUE) {
                out.writeByte(LONG_INSHORT);
                out.writeShort((int)lng);
            } else if (Integer.MIN_VALUE <= lng && lng <= Integer.MAX_VALUE) {
                out.writeByte(LONG_ININT);
                out.writeInt((int)lng);
            } else {
            out.writeByte(LONG);
                out.writeLong(lng);
            }
            break;

        case DataType.DATETIME:
            out.writeByte(DATETIME);
            out.writeLong(((DateTime) val).getMillis());
            out.writeShort(((DateTime) val).getZone().getOffset((DateTime) val) / ONE_MINUTE);
            break;

        case DataType.FLOAT:
            out.writeByte(FLOAT);
            out.writeFloat((Float) val);
            break;

        case DataType.BIGINTEGER:
            out.writeByte(BIGINTEGER);
            writeBigInteger(out, (BigInteger)val);
            break;

        case DataType.BIGDECIMAL:
            out.writeByte(BIGDECIMAL);
            writeBigDecimal(out, (BigDecimal)val);
            break;

        case DataType.DOUBLE:
            out.writeByte(DOUBLE);
            out.writeDouble((Double) val);
            break;

        case DataType.BOOLEAN:
            if ((Boolean) val)
                out.writeByte(BOOLEAN_TRUE);
            else
                out.writeByte(BOOLEAN_FALSE);
            break;

        case DataType.BYTE:
            out.writeByte(BYTE);
            out.writeByte((Byte) val);
            break;

        case DataType.BYTEARRAY: {
            DataByteArray bytes = (DataByteArray) val;
            SedesHelper.writeBytes(out, bytes.mData);
            break;

        }

        case DataType.CHARARRAY: {
            SedesHelper.writeChararray(out, (String) val);
            break;
        }
        case DataType.GENERIC_WRITABLECOMPARABLE:
            out.writeByte(GENERIC_WRITABLECOMPARABLE);
            // store the class name, so we know the class to create on read
            writeDatum(out, val.getClass().getName());
            Writable writable = (Writable) val;
            writable.write(out);
            break;

        case DataType.NULL:
            out.writeByte(NULL);
            break;

        default:
            throw new RuntimeException("Unexpected data type " + val.getClass().getName() + " found in stream. " +
                    "Note only standard Pig type is supported when you output from UDF/LoadFunc");
        }
    }

    private void writeMap(DataOutput out, Map<String, Object> m) throws IOException {

        final int sz = m.size();
        if (sz < UNSIGNED_BYTE_MAX) {
            out.writeByte(TINYMAP);
            out.writeByte(sz);
        } else if (sz < UNSIGNED_SHORT_MAX) {
            out.writeByte(SMALLMAP);
            out.writeShort(sz);
        } else {
            out.writeByte(MAP);
            out.writeInt(sz);
        }
        Iterator<Map.Entry<String, Object>> i = m.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, Object> entry = i.next();
            writeDatum(out, entry.getKey());
            writeDatum(out, entry.getValue());
        }
    }

    private void writeBag(DataOutput out, DataBag bag) throws IOException {
        // We don't care whether this bag was sorted or distinct because
        // using the iterator to write it will guarantee those things come
        // correctly. And on the other end there'll be no reason to waste
        // time re-sorting or re-applying distinct.
        final long sz = bag.size();
        if (sz < UNSIGNED_BYTE_MAX) {
            out.writeByte(TINYBAG);
            out.writeByte((int) sz);
        } else if (sz < UNSIGNED_SHORT_MAX) {
            out.writeByte(SMALLBAG);
            out.writeShort((int) sz);
        } else {
            out.writeByte(BAG);
            out.writeLong(sz);
        }

        Iterator<Tuple> it = bag.iterator();
        while (it.hasNext()) {
            writeTuple(out, it.next());
        }

    }

    private void writeTuple(DataOutput out, Tuple t) throws IOException {
        if (t instanceof TypeAwareTuple) {
            t.write(out);
        } else {
            SedesHelper.writeGenericTuple(out, t);
    }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.pig.data.InterSedes#addColsToTuple(java.io.DataInput, org.apache.pig.data.Tuple)
     */
    @Override
    public void addColsToTuple(DataInput in, Tuple t) throws IOException {
        byte type = in.readByte();
        int sz = getTupleSize(in, type);
        for (int i = 0; i < sz; i++) {
            t.append(readDatum(in));
        }
    }

    public static class BinInterSedesTupleRawComparator extends WritableComparator implements TupleRawComparator {

        private final Log mLog = LogFactory.getLog(getClass());
        private boolean[] mAsc;
        private boolean[] mSecondaryAsc;
        private static final boolean[] EMPTY_ASC = new boolean[] {};
        private boolean mWholeTuple;
        private boolean mIsSecondarySort;
        private boolean mHasNullField;
        private TupleFactory mFact;
        private InterSedes mSedes;

        public BinInterSedesTupleRawComparator() {
            super(BinSedesTuple.class);
        }

        @Override
        public Configuration getConf() {
            return null;
        }

        @Override
        public void setConf(Configuration conf) {
            if (!(conf instanceof JobConf)) {
                mLog.warn("Expected jobconf in setConf, got " + conf.getClass().getName());
                return;
            }
            try {
                mAsc = (boolean[]) ObjectSerializer.deserialize(conf.get("pig.sortOrder"));
                mSecondaryAsc = (boolean[]) ObjectSerializer.deserialize(conf.get("pig.secondarySortOrder"));
                mIsSecondarySort = true;
            } catch (IOException ioe) {
                mLog.error("Unable to deserialize sort order object" + ioe.getMessage());
                throw new RuntimeException(ioe);
            }
            if (mAsc == null) {
                mAsc = new boolean[1];
                mAsc[0] = true;
            }
            if (mSecondaryAsc == null) {
                mIsSecondarySort = false;
            }
            // If there's only one entry in mAsc, it means it's for the whole
            // tuple. So we can't be looking for each column.
            mWholeTuple = (mAsc.length == 1);
            mFact = TupleFactory.getInstance();
            mSedes = InterSedesFactory.getInterSedesInstance();
        }

        @Override
        public boolean hasComparedTupleNull() {
            return mHasNullField;
        }

        /**
         * Compare two BinSedesTuples as raw bytes. We assume the Tuples are NOT PigNullableWritable, so client classes
         * need to deal with Null and Index.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int rc = 0;
            ByteBuffer bb1 = ByteBuffer.wrap(b1, s1, l1);
            ByteBuffer bb2 = ByteBuffer.wrap(b2, s2, l2);
            try {
                rc = compareBinSedesTuple(bb1, bb2);
            } catch (IOException ioe) {
                mLog.error("I/O error during tuple comparison: " + ioe.getMessage());
                throw new RuntimeException(ioe.getMessage(), ioe);
            }
            return rc;
        }

        /**
         * Compare two BinSedesTuples as raw bytes. We deal with sort ordering in this method.
         *
         * @throws IOException
         */
        private int compareBinSedesTuple(ByteBuffer bb1, ByteBuffer bb2) throws IOException {
            mHasNullField = false;
            // store the position in case of deserialization
            int s1 = bb1.position();
            int s2 = bb2.position();
            // treat the outermost tuple differently because we have to deal with sort order
            int result = 0;
            try {
                // first compare sizes
                int tsz1 = readSize(bb1);
                int tsz2 = readSize(bb2);
                if (tsz1 > tsz2)
                    return 1;
                else if (tsz1 < tsz2)
                    return -1;
                else {
                    // if sizes are the same, compare field by field
                    if (mIsSecondarySort) {
                        // we have a compound tuple key (main_key, secondary_key). Each key has its own sort order, so
                        // we have to deal with them separately. We delegate it to the first invocation of
                        // compareBinInterSedesDatum()
                        assert (tsz1 == 2); // main_key, secondary_key
                        result = compareBinInterSedesDatum(bb1, bb2, mAsc);
                        if (result == 0)
                            result = compareBinInterSedesDatum(bb1, bb2, mSecondaryAsc);
                    } else {
                        // we have just one tuple key, we deal with sort order here
                        for (int i = 0; i < tsz1 && result == 0; i++) {
                            // EMPTY_ASC is used to distinguish original calls from recursive ones (hack-ish)
                            result = compareBinInterSedesDatum(bb1, bb2, EMPTY_ASC);
                            // flip if the order is descending
                            if (result != 0) {
                                if (!mWholeTuple && !mAsc[i])
                                    result *= -1;
                                else if (mWholeTuple && !mAsc[0])
                                    result *= -1;
                            }
                        }
                    }
                }
            } catch (UnsupportedEncodingException uee) {
                Tuple t1 = mFact.newTuple();
                Tuple t2 = mFact.newTuple();
                t1.readFields(new DataInputStream(new ByteArrayInputStream(bb1.array(), s1, bb1.limit())));
                t2.readFields(new DataInputStream(new ByteArrayInputStream(bb2.array(), s2, bb2.limit())));
                // delegate to compare()
                result = compare(t1, t2);
            }
            return result;
        }

        private int compareBinInterSedesDatum(ByteBuffer bb1, ByteBuffer bb2, boolean[] asc) throws IOException {
            int rc = 0;
            byte type1, type2;
            byte dt1 = bb1.get();
            byte dt2 = bb2.get();
            switch (dt1) {
            case BinInterSedes.NULL: {
                type1 = DataType.NULL;
                type2 = getGeneralizedDataType(dt2);
                if (asc != null) // we are scanning the top-level Tuple (original call)
                    mHasNullField = true;
                if (type1 == type2)
                    rc = 0;
                break;
            }
            case BinInterSedes.BOOLEAN_TRUE:
            case BinInterSedes.BOOLEAN_FALSE: {
                type1 = DataType.BOOLEAN;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    // false < true
                    int bv1 = (dt1 == BinInterSedes.BOOLEAN_TRUE) ? 1 : 0;
                    int bv2 = (dt2 == BinInterSedes.BOOLEAN_TRUE) ? 1 : 0;
                    rc = bv1 - bv2;
                }
                break;
            }
            case BinInterSedes.BYTE: {
                type1 = DataType.BYTE;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    byte bv1 = bb1.get();
                    byte bv2 = bb2.get();
                    rc = (bv1 < bv2 ? -1 : (bv1 == bv2 ? 0 : 1));
                }
                break;
            }
            case BinInterSedes.INTEGER_0:
            case BinInterSedes.INTEGER_1:
            case BinInterSedes.INTEGER_INBYTE:
            case BinInterSedes.INTEGER_INSHORT:
            case BinInterSedes.INTEGER: {
                type1 = DataType.INTEGER;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    int iv1 = readInt(bb1, dt1);
                    int iv2 = readInt(bb2, dt2);
                    rc = (iv1 < iv2 ? -1 : (iv1 == iv2 ? 0 : 1));
                }
                break;
            }
            case BinInterSedes.LONG_0:
            case BinInterSedes.LONG_1:
            case BinInterSedes.LONG_INBYTE:
            case BinInterSedes.LONG_INSHORT:
            case BinInterSedes.LONG_ININT:
            case BinInterSedes.LONG: {
                type1 = DataType.LONG;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    long lv1 = readLong(bb1, dt1);
                    long lv2 = readLong(bb2, dt2);
                    rc = (lv1 < lv2 ? -1 : (lv1 == lv2 ? 0 : 1));
                }
                break;
            }
            case BinInterSedes.DATETIME: {
                type1 = DataType.DATETIME;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    long lv1 = bb1.getLong();
                    bb1.position(bb1.position() + 2); // move cursor forward without read the timezone bytes
                    long lv2 = bb2.getLong();
                    bb2.position(bb2.position() + 2);
                    rc = (lv1 < lv2 ? -1 : (lv1 == lv2 ? 0 : 1));
                }
                break;
            }
            case BinInterSedes.FLOAT: {
                type1 = DataType.FLOAT;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    float fv1 = bb1.getFloat();
                    float fv2 = bb2.getFloat();
                    rc = Float.compare(fv1, fv2);
                }
                break;
            }
            case BinInterSedes.DOUBLE: {
                type1 = DataType.DOUBLE;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    double dv1 = bb1.getDouble();
                    double dv2 = bb2.getDouble();
                    rc = Double.compare(dv1, dv2);
                }
                break;
            }
            case BinInterSedes.BIGINTEGER: {
                type1 = DataType.BIGINTEGER;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    int sz1 = readSize(bb1, bb1.get());
                    int sz2 = readSize(bb2, bb2.get());
                    byte[] ca1 = new byte[sz1];
                    byte[] ca2 = new byte[sz2];
                    bb1.get(ca1);
                    bb2.get(ca2);
                    String str1 = null, str2 = null;
                    try {
                        str1 = new String(ca1, BinInterSedes.UTF8);
                        str2 = new String(ca2, BinInterSedes.UTF8);
                    } catch (UnsupportedEncodingException uee) {
                        mLog.warn("Unsupported string encoding", uee);
                        uee.printStackTrace();
                    }
                    if (str1 != null && str2 != null) {
                        rc = new BigInteger(str1).compareTo(new BigInteger(str2));
                    }
                }
                break;
            }
            case BinInterSedes.BIGDECIMAL: {
                type1 = DataType.BIGDECIMAL;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    int sz1 = readSize(bb1, bb1.get());
                    int sz2 = readSize(bb2, bb2.get());
                    byte[] ca1 = new byte[sz1];
                    byte[] ca2 = new byte[sz2];
                    bb1.get(ca1);
                    bb2.get(ca2);
                    String str1 = null, str2 = null;
                    try {
                        str1 = new String(ca1, BinInterSedes.UTF8);
                        str2 = new String(ca2, BinInterSedes.UTF8);
                    } catch (UnsupportedEncodingException uee) {
                        mLog.warn("Unsupported string encoding", uee);
                        uee.printStackTrace();
                    }
                    if (str1 != null && str2 != null) {
                        rc = new BigDecimal(str1).compareTo(new BigDecimal(str2));
                    }
                }
                break;
            }
            case BinInterSedes.TINYBYTEARRAY:
            case BinInterSedes.SMALLBYTEARRAY:
            case BinInterSedes.BYTEARRAY: {
                type1 = DataType.BYTEARRAY;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    int basz1 = readSize(bb1, dt1);
                    int basz2 = readSize(bb2, dt2);
                    rc = WritableComparator.compareBytes(
                          bb1.array(), bb1.position(), basz1,
                          bb2.array(), bb2.position(), basz2);
                    bb1.position(bb1.position() + basz1);
                    bb2.position(bb2.position() + basz2);
                }
                break;
            }
            case BinInterSedes.SMALLCHARARRAY:
            case BinInterSedes.CHARARRAY: {
                type1 = DataType.CHARARRAY;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    int casz1 = readSize(bb1, dt1);
                    int casz2 = readSize(bb2, dt2);
                    String str1 = null, str2 = null;
                    try {
                        str1 = new String(bb1.array(), bb1.position(), casz1, BinInterSedes.UTF8);
                        str2 = new String(bb2.array(), bb2.position(), casz2, BinInterSedes.UTF8);
                    } catch (UnsupportedEncodingException uee) {
                        mLog.warn("Unsupported string encoding", uee);
                        uee.printStackTrace();
                    } finally {
                        bb1.position(bb1.position() + casz1);
                        bb2.position(bb2.position() + casz2);
                    }
                    if (str1 != null && str2 != null)
                        rc = str1.compareTo(str2);
                }
                break;
            }
            case BinInterSedes.TUPLE_0:
            case BinInterSedes.TUPLE_1:
            case BinInterSedes.TUPLE_2:
            case BinInterSedes.TUPLE_3:
            case BinInterSedes.TUPLE_4:
            case BinInterSedes.TUPLE_5:
            case BinInterSedes.TUPLE_6:
            case BinInterSedes.TUPLE_7:
            case BinInterSedes.TUPLE_8:
            case BinInterSedes.TUPLE_9:
            case BinInterSedes.TINYTUPLE:
            case BinInterSedes.SMALLTUPLE:
            case BinInterSedes.TUPLE: {
                type1 = DataType.TUPLE;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2) {
                    // first compare sizes
                    int tsz1 = readSize(bb1, dt1);
                    int tsz2 = readSize(bb2, dt2);
                    if (tsz1 > tsz2)
                        return 1;
                    else if (tsz1 < tsz2)
                        return -1;
                    else {
                        // if sizes are the same, compare field by field. If we are doing secondary sort, use the sort
                        // order passed by the caller. Inner tuples never have sort order (so we pass null).
                        for (int i = 0; i < tsz1 && rc == 0; i++) {
                            rc = compareBinInterSedesDatum(bb1, bb2, null);
                            if (rc != 0 && asc != null && asc.length > 1 && !asc[i])
                                rc *= -1;
                        }
                    }
                }
                break;
            }
            case BinInterSedes.TINYBAG:
            case BinInterSedes.SMALLBAG:
            case BinInterSedes.BAG: {
                type1 = DataType.BAG;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2)
                    rc = compareBinInterSedesBag(bb1, bb2, dt1, dt2);
                break;
            }
            case BinInterSedes.TINYMAP:
            case BinInterSedes.SMALLMAP:
            case BinInterSedes.MAP: {
                type1 = DataType.MAP;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2)
                    rc = compareBinInterSedesMap(bb1, bb2, dt1, dt2);
                break;
            }
            case BinInterSedes.GENERIC_WRITABLECOMPARABLE: {
                type1 = DataType.GENERIC_WRITABLECOMPARABLE;
                type2 = getGeneralizedDataType(dt2);
                if (type1 == type2)
                    rc = compareBinInterSedesGenericWritableComparable(bb1, bb2);
                break;
            }
            default: {
                mLog.info("Unsupported DataType for binary comparison, switching to object deserialization: "
                        + DataType.genTypeToNameMap().get(dt1) + "(" + dt1 + ")");
                throw new UnsupportedEncodingException();
            }
            }
            // compare generalized data types
            if (type1 != type2)
                rc = (type1 < type2) ? -1 : 1;
            // apply sort order for keys that are not tuples or for whole tuples
            if (asc != null && asc.length == 1 && !asc[0])
                rc *= -1;
            return rc;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Tuple t1 = (Tuple) o1;
            Tuple t2 = (Tuple) o2;
            mHasNullField = false;
            // treat the outermost tuple differently because we have to deal with sort order
            int result = 0;

            // first compare sizes
            int tsz1 = t1.size();
            int tsz2 = t2.size();
            if (tsz1 > tsz2)
                return 1;
            else if (tsz1 < tsz2)
                return -1;
            else {
                try {
                    // if sizes are the same, compare field by field
                    if (mIsSecondarySort) {
                        // we have a compound tuple key (main_key, secondary_key). Each key has its own sort order, so
                        // we have to deal with them separately. We delegate it to the first invocation of
                        // compareDatum()
                        assert (tsz1 == 3); // main_key, secondary_key, value
                        result = compareDatum(t1.get(0), t2.get(0), mAsc);
                        if (result == 0)
                            result = compareDatum(t1.get(1), t2.get(1), mSecondaryAsc);
                    } else {
                        // we have just one tuple key and no chance of recursion, we delegate dealing with sort order to
                        // compareDatum()
                        result = compareDatum(t1, t2, mAsc);
                    }
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to compare tuples", e);
                }
            }
            return result;
        }

        private int compareDatum(Object o1, Object o2, boolean[] asc) {
            int rc = 0;
            if (o1 != null && o2 != null && o1 instanceof Tuple && o2 instanceof Tuple) {
                // objects are Tuples, we may need to apply sort order inside them
                Tuple t1 = (Tuple) o1;
                Tuple t2 = (Tuple) o2;
                int sz1 = t1.size();
                int sz2 = t2.size();
                if (sz2 < sz1) {
                    return 1;
                } else if (sz2 > sz1) {
                    return -1;
                } else {
                    for (int i = 0; i < sz1; i++) {
                        try {
                            rc = DataType.compare(t1.get(i), t2.get(i));
                            if (rc != 0 && asc != null && asc.length > 1 && !asc[i])
                                rc *= -1;
                            if (t1.get(i) == null) // (PIG-927) record if the tuple has a null field
                                mHasNullField = true;
                            if (rc!=0) break;
                        } catch (ExecException e) {
                            throw new RuntimeException("Unable to compare tuples", e);
                        }
                    }
                }
            } else {
                // objects are NOT Tuples, delegate to DataType.compare()
                rc = DataType.compare(o1, o2);
            }
            // apply sort order for keys that are not tuples or for whole tuples
            if (asc != null && asc.length == 1 && !asc[0])
                rc *= -1;
            return rc;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private int compareBinInterSedesGenericWritableComparable(ByteBuffer bb1, ByteBuffer bb2) throws ExecException,
                IOException {
            DataInputBuffer buffer1 = new DataInputBuffer();
            DataInputBuffer buffer2 = new DataInputBuffer();
            buffer1.reset(bb1.array(), bb1.position(), bb1.remaining());
            buffer2.reset(bb2.array(), bb2.position(), bb2.remaining());
            Comparable writable1 = (Comparable) mSedes.readDatum(buffer1);
            Comparable writable2 = (Comparable) mSedes.readDatum(buffer2);
            bb1.position(buffer1.getPosition());
            bb2.position(buffer2.getPosition());
            return writable1.compareTo(writable2);
        }

        @SuppressWarnings("unchecked")
        private int compareBinInterSedesBag(ByteBuffer bb1, ByteBuffer bb2, byte dt1, byte dt2) throws IOException {
            int s1 = bb1.position();
            int s2 = bb2.position();
            int l1 = bb1.remaining();
            int l2 = bb2.remaining();
            // first compare sizes
            int bsz1 = readSize(bb1, dt1);
            int bsz2 = readSize(bb2, dt2);
            if (bsz1 > bsz2)
                return 1;
            else if (bsz1 < bsz2)
                return -1;
            else {
                DataInputBuffer buffer1 = new DataInputBuffer();
                DataInputBuffer buffer2 = new DataInputBuffer();
                buffer1.reset(bb1.array(), s1, l1);
                buffer2.reset(bb2.array(), s2, l2);
                DataBag bag1 = (DataBag) mSedes.readDatum(buffer1, dt1);
                DataBag bag2 = (DataBag) mSedes.readDatum(buffer2, dt2);
                bb1.position(buffer1.getPosition());
                bb2.position(buffer2.getPosition());
                return bag1.compareTo(bag2);
            }
        }

        @SuppressWarnings("unchecked")
        private int compareBinInterSedesMap(ByteBuffer bb1, ByteBuffer bb2, byte dt1, byte dt2) throws ExecException,
                IOException {
            int s1 = bb1.position();
            int s2 = bb2.position();
            int l1 = bb1.remaining();
            int l2 = bb2.remaining();
            // first compare sizes
            int bsz1 = readSize(bb1, dt1);
            int bsz2 = readSize(bb2, dt2);
            if (bsz1 > bsz2)
                return 1;
            else if (bsz1 < bsz2)
                return -1;
            else {
                DataInputBuffer buffer1 = new DataInputBuffer();
                DataInputBuffer buffer2 = new DataInputBuffer();
                buffer1.reset(bb1.array(), s1, l1);
                buffer2.reset(bb2.array(), s2, l2);
                Map<String, Object> map1 = (Map<String, Object>) mSedes.readDatum(buffer1, dt1);
                Map<String, Object> map2 = (Map<String, Object>) mSedes.readDatum(buffer2, dt2);
                bb1.position(buffer1.getPosition());
                bb2.position(buffer2.getPosition());
                return DataType.compare(map1, map2, DataType.MAP, DataType.MAP);
            }
        }

        private static byte getGeneralizedDataType(byte type) {
            switch (type) {
            case BinInterSedes.NULL:
                return DataType.NULL;
            case BinInterSedes.BOOLEAN_TRUE:
            case BinInterSedes.BOOLEAN_FALSE:
                return DataType.BOOLEAN;
            case BinInterSedes.BYTE:
                return DataType.BYTE;
            case BinInterSedes.INTEGER_0:
            case BinInterSedes.INTEGER_1:
            case BinInterSedes.INTEGER_INBYTE:
            case BinInterSedes.INTEGER_INSHORT:
            case BinInterSedes.INTEGER:
                return DataType.INTEGER;
            case BinInterSedes.LONG_0:
            case BinInterSedes.LONG_1:
            case BinInterSedes.LONG_INBYTE:
            case BinInterSedes.LONG_INSHORT:
            case BinInterSedes.LONG_ININT:
            case BinInterSedes.LONG:
                return DataType.LONG;
            case BinInterSedes.DATETIME:
                return DataType.DATETIME;
            case BinInterSedes.FLOAT:
                return DataType.FLOAT;
            case BinInterSedes.DOUBLE:
                return DataType.DOUBLE;
            case BinInterSedes.BIGINTEGER:
                return DataType.BIGINTEGER;
            case BinInterSedes.BIGDECIMAL:
                return DataType.BIGDECIMAL;
            case BinInterSedes.TINYBYTEARRAY:
            case BinInterSedes.SMALLBYTEARRAY:
            case BinInterSedes.BYTEARRAY:
                return DataType.BYTEARRAY;
            case BinInterSedes.SMALLCHARARRAY:
            case BinInterSedes.CHARARRAY:
                return DataType.CHARARRAY;
            case BinInterSedes.TUPLE_0:
            case BinInterSedes.TUPLE_1:
            case BinInterSedes.TUPLE_2:
            case BinInterSedes.TUPLE_3:
            case BinInterSedes.TUPLE_4:
            case BinInterSedes.TUPLE_5:
            case BinInterSedes.TUPLE_6:
            case BinInterSedes.TUPLE_7:
            case BinInterSedes.TUPLE_8:
            case BinInterSedes.TUPLE_9:
            case BinInterSedes.TUPLE:
            case BinInterSedes.TINYTUPLE:
            case BinInterSedes.SMALLTUPLE:
                return DataType.TUPLE;
            case BinInterSedes.BAG:
            case BinInterSedes.TINYBAG:
            case BinInterSedes.SMALLBAG:
                return DataType.BAG;
            case BinInterSedes.MAP:
            case BinInterSedes.TINYMAP:
            case BinInterSedes.SMALLMAP:
                return DataType.MAP;
            case BinInterSedes.INTERNALMAP:
                return DataType.INTERNALMAP;
            case BinInterSedes.GENERIC_WRITABLECOMPARABLE:
                return DataType.GENERIC_WRITABLECOMPARABLE;
            default:
                throw new RuntimeException("Unexpected data type " + type + " found in stream.");
            }
        }

        private static long readLong(ByteBuffer bb, byte type) {
            int bytesToRead = 0;
            switch (type) {
            case BinInterSedes.LONG_0: return 0L;
            case BinInterSedes.LONG_1: return 1L;
            case BinInterSedes.LONG_INBYTE: return bb.get();
            case BinInterSedes.LONG_INSHORT: return bb.getShort();
            case BinInterSedes.LONG_ININT: return bb.getInt();
            case BinInterSedes.LONG: return bb.getLong();
            default:
                throw new RuntimeException("Unexpected data type " + type + " found in stream.");
            }
        }

        private static int readInt(ByteBuffer bb, byte type) {
            switch (type) {
            case BinInterSedes.INTEGER_0:
                return 0;
            case BinInterSedes.INTEGER_1:
                return 1;
            case BinInterSedes.INTEGER_INBYTE:
                return bb.get();
            case BinInterSedes.INTEGER_INSHORT:
                return bb.getShort();
            case BinInterSedes.INTEGER:
                return bb.getInt();
            default:
                throw new RuntimeException("Unexpected data type " + type + " found in stream.");
            }
        }

        /**
         * @param bb ByteBuffer having serialized object, including the type information
         * @param type serialized type information
         * @return the size of this type
         */
        private static int readSize(ByteBuffer bb) {
            return readSize(bb, bb.get());
        }

        /**
         * @param bb ByteBuffer having serialized object, minus the type information
         * @param type serialized type information
         * @return the size of this type
         */
        private static int readSize(ByteBuffer bb, byte type) {
            switch (type) {
            case BinInterSedes.TINYBYTEARRAY:
            case BinInterSedes.TINYTUPLE:
            case BinInterSedes.TINYBAG:
            case BinInterSedes.TINYMAP:
                return getUnsignedByte(bb);
            case BinInterSedes.SMALLBYTEARRAY:
            case BinInterSedes.SMALLCHARARRAY:
            case BinInterSedes.SMALLTUPLE:
            case BinInterSedes.SMALLBAG:
            case BinInterSedes.SMALLMAP:
                return getUnsignedShort(bb);
            case BinInterSedes.BYTEARRAY:
            case BinInterSedes.CHARARRAY:
            case BinInterSedes.TUPLE:
            case BinInterSedes.BAG:
            case BinInterSedes.MAP:
                return bb.getInt();
            case BinInterSedes.TUPLE_0:
                return 0;
            case BinInterSedes.TUPLE_1:
                return 1;
            case BinInterSedes.TUPLE_2:
                return 2;
            case BinInterSedes.TUPLE_3:
                return 3;
            case BinInterSedes.TUPLE_4:
                return 4;
            case BinInterSedes.TUPLE_5:
                return 5;
            case BinInterSedes.TUPLE_6:
                return 6;
            case BinInterSedes.TUPLE_7:
                return 7;
            case BinInterSedes.TUPLE_8:
                return 8;
            case BinInterSedes.TUPLE_9:
                return 9;
            default:
                throw new RuntimeException("Unexpected data type " + type + " found in stream.");
            }
        }

        //same as format used by DataInput/DataOutput for unsigned short
        private static int getUnsignedShort(ByteBuffer bb) {
            return (((bb.get() & 0xff) << 8) | (bb.get() & 0xff));
        }

        //same as format used by DataInput/DataOutput for unsigned byte
        private static int getUnsignedByte(ByteBuffer bb) {
            return bb.get() & 0xff;
        }
    }

    @Override
    public Class<? extends TupleRawComparator> getTupleRawComparatorClass() {
        return BinInterSedesTupleRawComparator.class;
    }

    public Tuple readTuple(DataInput in) throws IOException {
        return readTuple(in, in.readByte());
    }

    public static boolean isTupleByte(byte b) {
        return b == BinInterSedes.TUPLE
            || b == BinInterSedes.SMALLTUPLE
            || b == BinInterSedes.TINYTUPLE
            || b == BinInterSedes.SCHEMA_TUPLE
            || b == BinInterSedes.SCHEMA_TUPLE_BYTE_INDEX
            || b == BinInterSedes.SCHEMA_TUPLE_SHORT_INDEX
            || b == BinInterSedes.TUPLE_0
            || b == BinInterSedes.TUPLE_1
            || b == BinInterSedes.TUPLE_2
            || b == BinInterSedes.TUPLE_3
            || b == BinInterSedes.TUPLE_4
            || b == BinInterSedes.TUPLE_5
            || b == BinInterSedes.TUPLE_6
            || b == BinInterSedes.TUPLE_7
            || b == BinInterSedes.TUPLE_8
            || b == BinInterSedes.TUPLE_9;
    }
}
