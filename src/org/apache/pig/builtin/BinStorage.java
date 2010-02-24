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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BinStorageInputFormat;
import org.apache.pig.impl.io.BinStorageOutputFormat;
import org.apache.pig.impl.io.BinStorageRecordReader;
import org.apache.pig.impl.io.BinStorageRecordWriter;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.LogUtils;

public class BinStorage extends FileInputLoadFunc 
implements LoadCaster, StoreFuncInterface, LoadMetadata {

    
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

    @Override
    public Tuple getNext() throws IOException {
        if(recReader.nextKeyValue()) {
            return recReader.getCurrentValue();
        } else {
            return null;
        }
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            recWriter.write(null, t);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema schema){
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema schema) {
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
    
    @Override
    public InputFormat getInputFormat() {
        return new BinStorageInputFormat();
    }

    @Override
    public int hashCode() {
        return 42; 
    }

    @Override
    public LoadCaster getLoadCaster() {
        return this;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        recReader = (BinStorageRecordReader)reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public OutputFormat getOutputFormat() {
        return new BinStorageOutputFormat();
    }

    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.recWriter = (BinStorageRecordWriter) writer;        
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
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
    public String[] getPartitionKeys(String location, Configuration conf)
            throws IOException {
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Configuration conf)
            throws IOException {
        Properties props = ConfigurationUtil.toProperties(conf);
        // since local mode now is implemented as hadoop's local mode
        // we can treat either local or hadoop mode as hadoop mode - hence
        // we can use HDataStorage and FileLocalizer.openDFSFile below
        HDataStorage storage = new HDataStorage(props);
        if (!FileLocalizer.fileExists(location, storage)) {
            // At compile time in batch mode, the file may not exist
            // (such as intermediate file). Just return null - the
            // same way as we would if we did not get a valid record
            return null;
        }
        ReadToEndLoader loader = new ReadToEndLoader(this, conf, location, 0);
        // get the first record from the input file
        // and figure out the schema from the data in
        // the first record
        Tuple t = loader.getNext();
        if(t == null) {
            // we couldn't get a valid record from the input
            return null;
        }
        int numFields = t.size();
        Schema s = new Schema();
        for (int i = 0; i < numFields; i++) {
            try {
                s.add(DataType.determineFieldSchema(t.get(i)));
            } catch (Exception e) {
                int errCode = 2104;
                String msg = "Error while determining schema of BinStorage data.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            } 
        }
        return new ResourceSchema(s);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Configuration conf)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPartitionFilter(Expression plan) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
    }

}
