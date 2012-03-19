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
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.pig.StoreFunc;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BinStorageInputFormat;
import org.apache.pig.impl.io.BinStorageOutputFormat;
import org.apache.pig.impl.io.BinStorageRecordReader;
import org.apache.pig.impl.io.BinStorageRecordWriter;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.Utils;

/**
 * Load and store data in a binary format.  This class is used by Pig to move
 * data between MapReduce jobs.  Use of this function for storing user data is
 * supported.
 */
public class BinStorage extends FileInputLoadFunc 
implements StoreFuncInterface, LoadMetadata {

    static class UnImplementedLoadCaster implements LoadCaster {

        private static final String unImplementedErrorMessage = "Cannot cast bytes loaded from BinStorage. Please provide a custom converter.";
        @Override
        public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema)
                throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public String bytesToCharArray(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Double bytesToDouble(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Float bytesToFloat(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Integer bytesToInteger(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Long bytesToLong(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Boolean bytesToBoolean(byte[] b) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Map<String, Object> bytesToMap(byte[] b) throws IOException {
            return bytesToMap(b, null);
        }
        
        @Override
        public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }

        @Override
        public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema)
                throws IOException {
            throw new ExecException(unImplementedErrorMessage, 1118);
        }
    }

    Iterator<Tuple>     i              = null;
    private static final Log mLog = LogFactory.getLog(BinStorage.class);
    protected long                end            = Long.MAX_VALUE;
    
    static String casterString = null;
    static LoadCaster caster = null;
    
    private BinStorageRecordReader recReader = null;
    private BinStorageRecordWriter recWriter = null;
    
    public BinStorage() {
    }
    
    // If user knows how to cast the bytes for BinStorage, provide
    // the class name for the caster. When we later want to convert
    // bytes to other types, BinStorage knows how. This provides a way 
    // for user to store intermediate data without having to explicitly
    // list all the fields and figure out their parts.
    public BinStorage(String casterString) {
        this.casterString = casterString;
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
    
    public byte[] toBytes(Boolean b) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, b);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting boolean to bytes.";
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

    @SuppressWarnings("unchecked")
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        if (caster == null) {
            Class<LoadCaster> casterClass = null;
            if (casterString!=null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                try {
                    // Try casterString as a fully qualified name
                    casterClass = (Class<LoadCaster>)cl.loadClass(casterString);
                } catch (ClassNotFoundException e) {
                }
                if (casterClass==null) {
                    try {
                        // Try casterString as in builtin
                        casterClass = (Class<LoadCaster>)cl.loadClass("org.apache.pig.builtin." + casterString);
                    } catch (ClassNotFoundException e) {
                        throw new FrontendException("Cannot find LoadCaster class " + casterString, 1119, e); 
                    }
                }
                try {
                    caster = casterClass.newInstance();
                } catch (Exception e) {
                    throw new FrontendException("Cannot instantiate class " + casterString, 2259, e);
                }
            }
            else {
                caster = new UnImplementedLoadCaster();
            }
        }
        return caster;
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
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Configuration conf = job.getConfiguration();
        Properties props = ConfigurationUtil.toProperties(conf);
        
        // At compile time in batch mode, the file may not exist
        // (such as intermediate file). Just return null - the
        // same way as we would if we did not get a valid record
        String[] locations = getPathStrings(location);
        for (String loc : locations) {
            // since local mode now is implemented as hadoop's local mode
            // we can treat either local or hadoop mode as hadoop mode - hence
            // we can use HDataStorage and FileLocalizer.openDFSFile below
            HDataStorage storage;
            try {
            	storage = new HDataStorage((new org.apache.hadoop.fs.Path(loc)).toUri(), props);
            } catch (RuntimeException e) {
                throw new IOException(e);
            }
            if (!FileLocalizer.fileExists(loc, storage)) {
                return null;
            }
        }

        return Utils.getSchema(this, location, false, job);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression plan) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        StoreFunc.cleanupOnFailureImpl(location, job);
    }

}
