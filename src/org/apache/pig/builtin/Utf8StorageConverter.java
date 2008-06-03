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

import java.awt.image.VolatileImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.DataOutputBuffer;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;


/**
 * This abstract class provides standard conversions between utf8 encoded data
 * and pig data types.  It is intended to be extended by load and store
 * functions (such as PigStorage). 
 */
abstract public class Utf8StorageConverter {

    protected BagFactory mBagFactory = BagFactory.getInstance();
    protected TupleFactory mTupleFactory = TupleFactory.getInstance();
    protected final Log mLog = LogFactory.getLog(getClass());
        
    public Utf8StorageConverter() {
    }

    public DataBag bytesToBag(byte[] b) throws IOException {
        //TODO:FIXME
        return null;       
    }

    public String bytesToCharArray(byte[] b) throws IOException {
        return new String(b);
    }

    public Double bytesToDouble(byte[] b) throws IOException {
        try {
            return Double.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            mLog.warn("Unable to interpret value " + b + " in field being " +
                    "converted to double, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded");
            return null;
        }
    }

    public Float bytesToFloat(byte[] b) throws IOException {
        try {
            return Float.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            mLog.warn("Unable to interpret value " + b + " in field being " +
                    "converted to float, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded");
            return null;
        }
    }

    public Integer bytesToInteger(byte[] b) throws IOException {
        try {
            return Integer.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            mLog.warn("Unable to interpret value " + b + " in field being " +
                    "converted to int, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded");
            return null;
        }
    }

    public Long bytesToLong(byte[] b) throws IOException {
        try {
            return Long.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            mLog.warn("Unable to interpret value " + b + " in field being " +
                    "converted to long, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded");
            return null;
        }
    }

    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        //TODO:FIXME
        return null;
    }

    public Tuple bytesToTuple(byte[] b) throws IOException {
        return bytesToTuple(b, 0, b.length - 1);
    }

    private Tuple bytesToTuple(byte[] b, int start, int end) throws IOException {
        //TODO:FIXME
        return null;
    }
      

    public byte[] toBytes(DataBag bag) throws IOException {
        //TODO:FIXME
        throw new IOException("Conversion from Bag to bytes not supported");
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
        //TODO:FIXME
        throw new IOException("Conversion from Map to bytes not supported");
    }

    public byte[] toBytes(Tuple t) throws IOException {
        //TODO:FIXME
        throw new IOException("Conversion from Tuple to bytes not supported");  
    }
    

}
