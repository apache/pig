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

import java.io.IOException;
import java.util.Map;
import java.io.ByteArrayInputStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.parser.ParseException;
import org.apache.pig.data.parser.TextDataParser;
import org.apache.pig.impl.util.LogUtils;

/**
 * This abstract class provides standard conversions between utf8 encoded data
 * and pig data types.  It is intended to be extended by load and store
 * functions (such as PigStorage). 
 */
abstract public class Utf8StorageConverter {

    protected BagFactory mBagFactory = BagFactory.getInstance();
    protected TupleFactory mTupleFactory = TupleFactory.getInstance();
    protected final Log mLog = LogFactory.getLog(getClass());

    private Integer mMaxInt = new Integer(Integer.MAX_VALUE);
    private Long mMaxLong = new Long(Long.MAX_VALUE);
    private TextDataParser dataParser = null;
    
    private PigLogger pigLogger = PhysicalOperator.getPigLogger();
        
    public Utf8StorageConverter() {
    }

    private Object parseFromBytes(byte[] b) throws ParseException {
        ByteArrayInputStream in = new ByteArrayInputStream(b);
        if(dataParser == null) {
            dataParser = new TextDataParser(in);
        } else {
            dataParser.ReInit(in);
        }
        return dataParser.Parse();
    }

    public DataBag bytesToBag(byte[] b) throws IOException {
        if(b == null)
            return null;
        DataBag db;
        try {
            db = (DataBag)parseFromBytes(b);
        } catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type bag, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type bag, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return (DataBag)db;
    }

    public String bytesToCharArray(byte[] b) throws IOException {
        if(b == null)
            return null;
        return new String(b, "UTF-8");
    }

    public Double bytesToDouble(byte[] b) {
        if(b == null)
            return null;
        try {
            return Double.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to double, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }

    public Float bytesToFloat(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s;
        if(b.length > 0 && 
           (b[b.length - 1] == 'F' || b[b.length - 1] == 'f') ){
            s = new String(b, 0, b.length - 1);
        } 
        else {
            s = new String(b);
        }
        
        try {
            return Float.valueOf(s);
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to float, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }

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
                if (d.doubleValue() > mMaxInt.doubleValue() + 1.0) {
                    LogUtils.warn(this, "Value " + d + " too large for integer", 
                                PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return new Integer(d.intValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                        "converted to int, caught NumberFormatException <" +
                        nfe.getMessage() + "> field discarded", 
                        PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }

    public Long bytesToLong(byte[] b) throws IOException {
        if(b == null)
            return null;

        String s;
        if(b.length > 0  &&  
           (b[b.length - 1] == 'L' || b[b.length - 1] == 'l') ){
            s = new String(b, 0, b.length - 1);
        } 
        else {
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
                if (d.doubleValue() > mMaxLong.doubleValue() + 1.0) {
                	LogUtils.warn(this, "Value " + d + " too large for integer", 
                	            PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return new Long(d.longValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                            "converted to long, caught NumberFormatException <" +
                            nfe.getMessage() + "> field discarded", 
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }

    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        if(b == null)
            return null;
        Map<Object, Object> map;
        try {
            map = (Map<Object, Object>)parseFromBytes(b);
        }
        catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type map, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type map, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return map;
    }

    public Tuple bytesToTuple(byte[] b) throws IOException {
        if(b == null)
            return null;
        Tuple t;
        try {
            t = (Tuple)parseFromBytes(b);
        } 
        catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type tuple, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type tuple, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return t;
    }


    public byte[] toBytes(DataBag bag) throws IOException {
        return bag.toString().getBytes();
    }

    public byte[] toBytes(String s) throws IOException {
        return s.getBytes();
    }

    public byte[] toBytes(Double d) throws IOException {
        return d.toString().getBytes();
    }

    public byte[] toBytes(Float f) throws IOException {
        return f.toString().getBytes();
    }

    public byte[] toBytes(Integer i) throws IOException {
        return i.toString().getBytes();
    }

    public byte[] toBytes(Long l) throws IOException {
        return l.toString().getBytes();
    }

    public byte[] toBytes(Map<Object, Object> m) throws IOException {
        return DataType.mapToString(m).getBytes();
    }

    public byte[] toBytes(Tuple t) throws IOException {
        return t.toString().getBytes();
    }
    


}
