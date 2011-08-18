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
package org.apache.pig.builtin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.EmptyStackException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.LogUtils;

/**
 * This abstract class provides standard conversions between utf8 encoded data
 * and pig data types.  It is intended to be extended by load and store
 * functions (such as {@link PigStorage}). 
 */
public class Utf8StorageConverter implements LoadStoreCaster {

    protected BagFactory mBagFactory = BagFactory.getInstance();
    protected TupleFactory mTupleFactory = TupleFactory.getInstance();
    protected final Log mLog = LogFactory.getLog(getClass());

    private static final Integer mMaxInt = Integer.valueOf(Integer.MAX_VALUE);
    private static final Integer mMinInt = Integer.valueOf(Integer.MIN_VALUE);
    private static final Long mMaxLong = Long.valueOf(Long.MAX_VALUE);
    private static final Long mMinLong = Long.valueOf(Long.MIN_VALUE);
    private static final int BUFFER_SIZE = 1024;
        
    public Utf8StorageConverter() {
    }

    private char findStartChar(char start) throws IOException{
        switch (start) {
        case ')': return '(';
        case ']': return '[';
        case '}': return '{';
        default: throw new IOException("Unknown start character");
        }
    }
    
    private DataBag consumeBag(PushbackInputStream in, ResourceFieldSchema fieldSchema) throws IOException {
        if (fieldSchema==null) {
            throw new IOException("Schema is null");
        }
        ResourceFieldSchema[] fss=fieldSchema.getSchema().getFields();
        Tuple t;
        int buf;
        while ((buf=in.read())!='{') {
            if (buf==-1) {
                throw new IOException("Unexpect end of bag");
            }
        }
        if (fss.length!=1)
            throw new IOException("Only tuple is allowed inside bag schema");
        ResourceFieldSchema fs = fss[0];
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        while (true) {
            t = consumeTuple(in, fs);
            if (t!=null)
                db.add(t);
            while ((buf=in.read())!='}'&&buf!=',') {
                if (buf==-1) {
                    throw new IOException("Unexpect end of bag");
                }
            }
            if (buf=='}')
                break;
        }
        return db;
    }
    
    private Tuple consumeTuple(PushbackInputStream in, ResourceFieldSchema fieldSchema) throws IOException {
        if (fieldSchema==null) {
            throw new IOException("Schema is null");
        }
        int buf;
        ByteArrayOutputStream mOut;
        
        while ((buf=in.read())!='('||buf=='}') {
            if (buf==-1) {
                throw new IOException("Unexpect end of tuple");
            }
            if (buf=='}') {
                in.unread(buf);
                return null;
            }
        }
        Tuple t = TupleFactory.getInstance().newTuple();
        if (fieldSchema.getSchema()!=null && fieldSchema.getSchema().getFields().length!=0) {
            ResourceFieldSchema[] fss = fieldSchema.getSchema().getFields();
            // Interpret item inside tuple one by one based on the inner schema
            for (int i=0;i<fss.length;i++) {
                Object field;
                ResourceFieldSchema fs = fss[i];
                int delimit = ',';
                if (i==fss.length-1)
                    delimit = ')';
                
                if (DataType.isComplex(fs.getType())) {
                    field = consumeComplexType(in, fs);
                    while ((buf=in.read())!=delimit) {
                        if (buf==-1) {
                            throw new IOException("Unexpect end of tuple");
                        }
                    }
                }
                else {
                    mOut = new ByteArrayOutputStream(BUFFER_SIZE);
                    while ((buf=in.read())!=delimit) {
                        if (buf==-1) {
                            throw new IOException("Unexpect end of tuple");
                        }
                        if (buf==delimit)
                            break;
                        mOut.write(buf);
                    }
                    field = parseSimpleType(mOut.toByteArray(), fs);
                }
                t.append(field);
            }
        }
        else {
            // No inner schema, treat everything inside tuple as bytearray
            Stack<Character> level = new Stack<Character>();  // keep track of nested tuple/bag/map. We do not interpret, save them as bytearray
            mOut = new ByteArrayOutputStream(BUFFER_SIZE);
            while (true) {
                buf=in.read();
                if (buf==-1) {
                    throw new IOException("Unexpect end of tuple");
                }
                if (buf=='['||buf=='{'||buf=='(') {
                    level.push((char)buf);
                    mOut.write(buf);
                }
                else if (buf==')' && level.isEmpty()) // End of tuple
                {
                    DataByteArray value = new DataByteArray(mOut.toByteArray());
                    t.append(value);
                    break;
                }
                else if (buf==',' && level.isEmpty())
                {
                    DataByteArray value = new DataByteArray(mOut.toByteArray());
                    t.append(value);
                    mOut.reset();
                }
                else if (buf==']' ||buf=='}'||buf==')')
                {
                    if (level.peek()==findStartChar((char)buf))
                        level.pop();
                    else
                        throw new IOException("Malformed tuple");
                    mOut.write(buf);
                }
                else
                    mOut.write(buf);
            }
        }
        return t;
    }
    
    private Map<String, Object> consumeMap(PushbackInputStream in, ResourceFieldSchema fieldSchema) throws IOException {
        int buf;
        
        while ((buf=in.read())!='[') {
            if (buf==-1) {
                throw new IOException("Unexpect end of map");
            }
        }
        HashMap<String, Object> m = new HashMap<String, Object>();
        ByteArrayOutputStream mOut = new ByteArrayOutputStream(BUFFER_SIZE);
        while (true) {
            // Read key (assume key can not contains special character such as #, (, [, {, }, ], )
            while ((buf=in.read())!='#') {
                if (buf==-1) {
                    throw new IOException("Unexpect end of map");
                }
                mOut.write(buf);
            }
            String key = bytesToCharArray(mOut.toByteArray());
            if (key.length()==0)
                throw new IOException("Map key can not be null");
            
            // Read value
            mOut.reset();
            Stack<Character> level = new Stack<Character>(); // keep track of nested tuple/bag/map. We do not interpret, save them as bytearray
            while (true) {
                buf=in.read();
                if (buf==-1) {
                    throw new IOException("Unexpect end of map");
                }
                if (buf=='['||buf=='{'||buf=='(') {
                    level.push((char)buf);
                }
                else if (buf==']' && level.isEmpty()) // End of map
                    break;
                else if (buf==']' ||buf=='}'||buf==')')
                {
                    try {
                        if (level.peek()==findStartChar((char)buf))
                            level.pop();
                    } catch (EmptyStackException e) {
                        throw new IOException("Malformed map");
                    }
                } else if (buf==','&&level.isEmpty()) { // Current map item complete
                    break;
                }
                mOut.write(buf);
            }
            Object value = null;
            if (fieldSchema!=null && fieldSchema.getSchema()!=null && mOut.size()>0) {
                value = bytesToObject(mOut.toByteArray(), fieldSchema.getSchema().getFields()[0]);
            } else if (mOut.size()>0) { // untyped map
                value = new DataByteArray(mOut.toByteArray());
            }
            m.put(key, value);
            mOut.reset();
            if (buf==']')
                break;
        }
        return m;
    }
    
    private Object bytesToObject(byte[] b, ResourceFieldSchema fs) throws IOException {
        Object field;
        if (DataType.isComplex(fs.getType())) {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            PushbackInputStream in = new PushbackInputStream(bis);
            field = consumeComplexType(in, fs);
        }
        else {
            field = parseSimpleType(b, fs);
        }
        return field;
    }
    
    private Object consumeComplexType(PushbackInputStream in, ResourceFieldSchema complexFieldSchema) throws IOException {
        Object field;
        switch (complexFieldSchema.getType()) {
        case DataType.BAG:
            field = consumeBag(in, complexFieldSchema);
            break;
        case DataType.TUPLE:
            field = consumeTuple(in, complexFieldSchema);
            break;
        case DataType.MAP:
            field = consumeMap(in, complexFieldSchema);
            break;
        default:
            throw new IOException("Unknown complex data type");
        }
        return field;
    }
    
    private Object parseSimpleType(byte[] b, ResourceFieldSchema simpleFieldSchema) throws IOException {
        Object field;
        switch (simpleFieldSchema.getType()) {
        case DataType.INTEGER:
            field = bytesToInteger(b);
            break;
        case DataType.LONG:
            field = bytesToLong(b);
            break;
        case DataType.FLOAT:
            field = bytesToFloat(b);
            break;
        case DataType.DOUBLE:
            field = bytesToDouble(b);
            break;
        case DataType.CHARARRAY:
            field = bytesToCharArray(b);
            break;
        case DataType.BYTEARRAY:
            field = new DataByteArray(b);
            break;
        case DataType.BOOLEAN:
            field = bytesToBoolean(b);
            break;
        default:
            throw new IOException("Unknown simple data type");
        }
        return field;
    }

    @Override
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema schema) throws IOException {
        if(b == null)
            return null;
        DataBag db;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            PushbackInputStream in = new PushbackInputStream(bis);
            db = consumeBag(in, schema);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                    "converted to type bag, caught ParseException <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
        return db;
    }

    @Override
    public String bytesToCharArray(byte[] b) throws IOException {
        if(b == null)
            return null;
        return new String(b, "UTF-8");
    }

    @Override
    public Double bytesToDouble(byte[] b) {
        if(b == null)
            return null;
        try {
            return Double.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                    "converted to double, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }
    
    @Override
    public Float bytesToFloat(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s;
        if (b.length > 0 && (b[b.length - 1] == 'F' || b[b.length - 1] == 'f')) {
            s = new String(b, 0, b.length - 1);
        } else {
            s = new String(b);
        }

        try {
            return Float.valueOf(s);
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                    "converted to float, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }
    

    @Override
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s = new String(b);
        if (s.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        } else if (s.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        } else {
            return null;
        }
    }

    @Override
    public Integer bytesToInteger(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s = new String(b);
        try {
            return Integer.valueOf(s);
        } catch (NumberFormatException nfe) {
            // It's possible that this field can be interpreted as a double.
            // Unfortunately Java doesn't handle this in Integer.valueOf.  So
            // we need to try to convert it to a double and if that works then
            // go to an int.
            try {
                Double d = Double.valueOf(s);
                // Need to check for an overflow error
                if (Double.compare(d.doubleValue(), mMaxInt.doubleValue() + 1) >= 0 ||
                        Double.compare(d.doubleValue(), mMinInt.doubleValue() - 1) <= 0) {
                    LogUtils.warn(this, "Value " + d + " too large for integer", 
                            PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return Integer.valueOf(d.intValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                        "converted to int, caught NumberFormatException <" +
                        nfe.getMessage() + "> field discarded", 
                        PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }
    
    @Override
    public Long bytesToLong(byte[] b) throws IOException {
        if (b == null)
            return null;
        String s;
        if (b.length > 0 && (b[b.length - 1] == 'L' || b[b.length - 1] == 'l')) {
            s = new String(b, 0, b.length - 1);
        } else {
            s = new String(b);
        }
        
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException nfe) {
            // It's possible that this field can be interpreted as a double.
            // Unfortunately Java doesn't handle this in Long.valueOf.  So
            // we need to try to convert it to a double and if that works then
            // go to an long.
            try {
                Double d = Double.valueOf(s);
                // Need to check for an overflow error
                if (Double.compare(d.doubleValue(), mMaxLong.doubleValue() + 1) > 0 ||
                        Double.compare(d.doubleValue(), mMinLong.doubleValue() - 1) < 0) {
                	LogUtils.warn(this, "Value " + d + " too large for long", 
                	            PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return Long.valueOf(d.longValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                            "converted to long, caught NumberFormatException <" +
                            nfe.getMessage() + "> field discarded", 
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
        if(b == null)
            return null;
        Map<String, Object> map;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            PushbackInputStream in = new PushbackInputStream(bis);
            map = consumeMap(in, fieldSchema);
        }
        catch (IOException e) {
            LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                    "converted to type map, caught ParseException <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return map;
    }
    
    @Override
    public Map<String, Object> bytesToMap(byte[] b) throws IOException {
        return bytesToMap(b, null);
    }

    @Override
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
        if(b == null)
            return null;
        Tuple t;
        
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            PushbackInputStream in = new PushbackInputStream(bis);
            t = consumeTuple(in, fieldSchema);
        } 
        catch (IOException e) {
            LogUtils.warn(this, "Unable to interpret value " + Arrays.toString(b) + " in field being " +
                    "converted to type tuple, caught ParseException <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }

        return t;
    }

    @Override
    public byte[] toBytes(DataBag bag) throws IOException {
        return bag.toString().getBytes();
    }

    @Override
    public byte[] toBytes(String s) throws IOException {
        return s.getBytes();
    }

    @Override
    public byte[] toBytes(Double d) throws IOException {
        return d.toString().getBytes();
    }

    @Override
    public byte[] toBytes(Float f) throws IOException {
        return f.toString().getBytes();
    }

    @Override
    public byte[] toBytes(Integer i) throws IOException {
        return i.toString().getBytes();
    }

    @Override
    public byte[] toBytes(Long l) throws IOException {
        return l.toString().getBytes();
    }

    @Override
    public byte[] toBytes(Boolean b) throws IOException {
        return b.toString().getBytes();
    }

    @Override
    public byte[] toBytes(Map<String, Object> m) throws IOException {
        return DataType.mapToString(m).getBytes();
    }

    @Override
    public byte[] toBytes(Tuple t) throws IOException {
        return t.toString().getBytes();
    }

    @Override
    public byte[] toBytes(DataByteArray a) throws IOException {
        return a.get();
    }

}
