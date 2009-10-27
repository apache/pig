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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BinStorageInputFormat;
import org.apache.pig.impl.io.BinStorageOutputFormat;
import org.apache.pig.impl.io.BinStorageRecordReader;
import org.apache.pig.impl.io.BinStorageRecordWriter;
import org.apache.pig.impl.util.LogUtils;

public class BinStorage implements ReversibleLoadStoreFunc, LoadCaster {
    public static final int RECORD_1 = 0x01;
    public static final int RECORD_2 = 0x02;
    public static final int RECORD_3 = 0x03;

    Iterator<Tuple>     i              = null;
    private static final Log mLog = LogFactory.getLog(BinStorage.class);
    protected long                end            = Long.MAX_VALUE;
    
    private BinStorageRecordReader recReader = null;
    private BinStorageRecordWriter recWriter = null;
    
    /**
     * Simple binary nested reader format
     */
    public BinStorage() {
    }

    public Tuple getNext() throws IOException {
        if(recReader.nextKeyValue()) {
            return recReader.getCurrentValue();
        } else {
            return null;
        }
    }

    public void putNext(Tuple t) throws IOException {
        try {
            recWriter.write(null, t);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public DataBag bytesToBag(byte[] b){
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToBag(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to bag, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }        
    }

    public String bytesToCharArray(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToCharArray(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to chararray, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Double bytesToDouble(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Double(dis.readDouble());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to double, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Float bytesToFloat(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Float(dis.readFloat());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to float, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
            
            return null;
        }
    }

    public Integer bytesToInteger(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return Integer.valueOf(dis.readInt());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to integer, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Long bytesToLong(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return Long.valueOf(dis.readLong());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to long, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Map<String, Object> bytesToMap(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToMap(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to map, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Tuple bytesToTuple(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToTuple(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to tuple, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public byte[] toBytes(DataBag bag) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, bag);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting bag to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(String s) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, s);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting chararray to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Double d) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, d);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting double to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Float f) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, f);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting float to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Integer i) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, i);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting int to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Long l) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, l);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting long to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Map<String, Object> m) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, m);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting map to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Tuple t) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, t);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting tuple to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#doneReading()
     */
    @Override
    public void doneReading() {
        // nothing to be done for now
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() {
        return new BinStorageInputFormat();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getLoadCaster()
     */
    @Override
    public LoadCaster getLoadCaster() {
        return this;
    }

    /* (non-Javadoc)
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        recReader = (BinStorageRecordReader)reader;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#allFinished(org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void allFinished(Job job) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#doneWriting()
     */
    @Override
    public void doneWriting() {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#getOutputFormat()
     */
    @Override
    public OutputFormat getOutputFormat() {
        return new BinStorageOutputFormat();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
     */
    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.recWriter = (BinStorageRecordWriter) writer;        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#setSchema(org.apache.pig.ResourceSchema)
     */
    @Override
    public void setSchema(ResourceSchema s) throws IOException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#setStoreLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }
}
