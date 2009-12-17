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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a regular expression. See String.split(delimiter) and
 * http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html for more information.
 */
public class PigStorage extends FileInputLoadFunc implements StoreFunc, 
LoadPushDown {
    protected RecordReader in = null;
    protected RecordWriter writer = null;
    protected final Log mLog = LogFactory.getLog(getClass());
    private String signature;
        
    long end = Long.MAX_VALUE;
    private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final int OS_UNIX = 0;
    private static final int OS_WINDOWS = 1;
    private static final String UTF8 = "UTF-8";
    private static final int BUFFER_SIZE = 1024;
    
    public PigStorage() {
    }
    
    private boolean[] mRequiredColumns = null;
    
    private boolean mRequiredColumnsInitialized = false;

    /**
     * Constructs a Pig loader that uses specified regex as a field delimiter.
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("\t" is the default.)
     */
    public PigStorage(String delimiter) {
        this();
        if (delimiter.length() == 1) {
            this.fieldDel = (byte)delimiter.charAt(0);
        } else if (delimiter.length() > 1 && delimiter.charAt(0) == '\\') {
            switch (delimiter.charAt(1)) {
            case 't':
                this.fieldDel = (byte)'\t';
                break;

            case 'x':
            case 'u':
                this.fieldDel =
                    Integer.valueOf(delimiter.substring(2)).byteValue();
                break;

            default:                
                throw new RuntimeException("Unknown delimiter " + delimiter);
            }
        } else {            
            throw new RuntimeException("PigStorage delimeter must be a single character");
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }                                                                                           
            Text value = (Text) in.getCurrentValue();
            byte[] buf = value.getBytes();
            int len = value.getLength();
            int start = 0;
            int fieldID = 0;
            for (int i = 0; i < len; i++) {
                if (buf[i] == fieldDel) {
                    if (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))
                        readField(buf, start, i);
                    start = i + 1;
                    fieldID++;
                }
            }
            // pick up the last field
            if (start <= len && (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID]))) {
                readField(buf, start, len);
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, 
                    PigException.REMOTE_ENVIRONMENT, e);
        }
      
    }

    ByteArrayOutputStream mOut = new ByteArrayOutputStream(BUFFER_SIZE);

    private void putField(Object field) throws IOException {
        //string constants for each delimiter
        String tupleBeginDelim = "(";
        String tupleEndDelim = ")";
        String bagBeginDelim = "{";
        String bagEndDelim = "}";
        String mapBeginDelim = "[";
        String mapEndDelim = "]";
        String fieldDelim = ",";
        String mapKeyValueDelim = "#";

        switch (DataType.findType(field)) {
        case DataType.NULL:
            break; // just leave it empty

        case DataType.BOOLEAN:
            mOut.write(((Boolean)field).toString().getBytes());
            break;

        case DataType.INTEGER:
            mOut.write(((Integer)field).toString().getBytes());
            break;

        case DataType.LONG:
            mOut.write(((Long)field).toString().getBytes());
            break;

        case DataType.FLOAT:
            mOut.write(((Float)field).toString().getBytes());
            break;

        case DataType.DOUBLE:
            mOut.write(((Double)field).toString().getBytes());
            break;

        case DataType.BYTEARRAY: {
            byte[] b = ((DataByteArray)field).get();
            mOut.write(b, 0, b.length);
            break;
                                 }

        case DataType.CHARARRAY:
            // oddly enough, writeBytes writes a string
            mOut.write(((String)field).getBytes(UTF8));
            break;

        case DataType.MAP:
            boolean mapHasNext = false;
            Map<String, Object> m = (Map<String, Object>)field;
            mOut.write(mapBeginDelim.getBytes(UTF8));
            for(Map.Entry<String, Object> e: m.entrySet()) {
                if(mapHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    mapHasNext = true;
                }
                putField(e.getKey());
                mOut.write(mapKeyValueDelim.getBytes(UTF8));
                putField(e.getValue());
            }
            mOut.write(mapEndDelim.getBytes(UTF8));
            break;

        case DataType.TUPLE:
            boolean tupleHasNext = false;
            Tuple t = (Tuple)field;
            mOut.write(tupleBeginDelim.getBytes(UTF8));
            for(int i = 0; i < t.size(); ++i) {
                if(tupleHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    tupleHasNext = true;
                }
                try {
                    putField(t.get(i));
                } catch (ExecException ee) {
                    throw ee;
                }
            }
            mOut.write(tupleEndDelim.getBytes(UTF8));
            break;

        case DataType.BAG:
            boolean bagHasNext = false;
            mOut.write(bagBeginDelim.getBytes(UTF8));
            Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
            while(tupleIter.hasNext()) {
                if(bagHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    bagHasNext = true;
                }
                putField((Object)tupleIter.next());
            }
            mOut.write(bagEndDelim.getBytes(UTF8));
            break;
            
        default: {
            int errCode = 2108;
            String msg = "Could not determine data type of field: " + field;
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        }
    }

    @Override
    public void putNext(Tuple f) throws IOException {
        // I have to convert integer fields to string, and then to bytes.
        // If I use a DataOutputStream to convert directly from integer to
        // bytes, I don't get a string representation.
        int sz = f.size();
        for (int i = 0; i < sz; i++) {
            Object field;
            try {
                field = f.get(i);
            } catch (ExecException ee) {
                throw ee;
            }

            putField(field);

            if (i != sz - 1) {
                mOut.write(fieldDel);
            }
        }
        Text text = new Text(mOut.toByteArray());
        try {
            writer.write(null, text);
            mOut.reset();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private void readField(byte[] buf, int start, int end) {
        if (mProtoTuple == null) {
            mProtoTuple = new ArrayList<Object>();
        }

        if (start == end) {
            // NULL value
            mProtoTuple.add(null);
        } else {
            mProtoTuple.add(new DataByteArray(buf, start, end));
        }
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        signature = requiredFieldList.getSignature();
        if (!requiredFieldList.isAllFieldsRequired())
        {
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
                else
                    mRequiredColumns[rf.getIndex()] = false;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PigStorage)
            return equals((PigStorage)obj);
        else
            return false;
    }

    public boolean equals(PigStorage other) {
        return this.fieldDel == other.fieldDel;
    }

    @Override
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public OutputFormat getOutputFormat() {
        return new TextOutputFormat<WritableComparable, Text>();
    }

    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.writer = writer;        
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {

    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
            throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @Override
    public int hashCode() {
        return (int)fieldDel;
    }

    
    @Override
    public void setSignature(String signature) {
        this.signature = signature; 
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

}
