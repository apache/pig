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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * The main split class that maintains important
 * information about the input split.
 *
 * The reason this class implements Configurable is so that Hadoop will call
 * {@link Configurable#setConf(Configuration)} on the backend so we can use
 * the Configuration to create the SerializationFactory to deserialize the
 * wrapped InputSplit.
 */
public class PigSplit extends InputSplit implements Writable, Configurable {
    //The operators to which the tuples from this
    //input file are attached. These are the successors
    //of the load operator representing this input
    private ArrayList<OperatorKey> targetOps;

    // index starting from 0 representing the input number
    // So if we have 3 inputs (say for a 3 way join), then the 
    // splits corresponding to the first input will have an index of 0, those
    // corresponding to the second will have an index of 1 and so on
    // This will be used to get the LoadFunc corresponding to the input
    // in PigInputFormat and related code.
    private int inputIndex;
    
    // The real InputSplit this split is wrapping
    private InputSplit wrappedSplit;

    // index of the wrappedSplit in the list of splits returned by
    // InputFormat.getSplits()
    // This will be used by MergeJoinIndexer to record the split # in the
    // index
    private int splitIndex;
    
    /**
     * the job Configuration
     */
    private Configuration conf;
    
    /**
     * total number of splits - required by skew join
     */
    private int totalSplits;

    // this seems necessary for Hadoop to instatiate this split on the
    // backend
    public PigSplit() {}
    
    public PigSplit(InputSplit wrappedSplit, int inputIndex, 
            List<OperatorKey> targetOps, int splitIndex) {
        this.wrappedSplit = wrappedSplit;
        this.inputIndex = inputIndex;
        this.targetOps = new ArrayList<OperatorKey>(targetOps);
        this.splitIndex = splitIndex;
    }


    public List<OperatorKey> getTargetOps() {
        return new ArrayList<OperatorKey>(targetOps);
    }
    

    /**
     * This methods returns the actual InputSplit (as returned by the 
     * {@link InputFormat}) which this class is wrapping.
     * @return the wrappedSplit
     */
    public InputSplit getWrappedSplit() {
        return wrappedSplit;
    }
    
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
            return wrappedSplit.getLocations();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return wrappedSplit.getLength();
    }
    
    public void readFields(DataInput is) throws IOException {
        splitIndex = is.readInt();
        inputIndex = is.readInt();
        targetOps = (ArrayList<OperatorKey>) readObject(is);
        String splitClassName = is.readUTF();
        try {
            Class splitClass = conf.getClassByName(splitClassName);
            wrappedSplit = (InputSplit) 
            ReflectionUtils.newInstance(splitClass, conf);
            SerializationFactory sf = new SerializationFactory(conf);
            Deserializer d = sf.getDeserializer(splitClass);
            d.open((InputStream) is);
            d.deserialize(wrappedSplit);
            d.close();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        
    }

    public void write(DataOutput os) throws IOException {
        os.writeInt(splitIndex);
        os.writeInt(inputIndex);
        writeObject(targetOps, os);
        os.writeUTF(wrappedSplit.getClass().getName());
        SerializationFactory sf = new SerializationFactory(conf);
        Serializer s = 
            sf.getSerializer(wrappedSplit.getClass());
        s.open((OutputStream) os);
        s.serialize(wrappedSplit);
        s.close();
        
    }

    private void writeObject(Serializable obj, DataOutput os)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        byte[] bytes = baos.toByteArray();
        os.writeInt(bytes.length);
        os.write(bytes);
    }

    private Object readObject(DataInput is) throws IOException {
        byte[] bytes = new byte[is.readInt()];
        is.readFully(bytes);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                bytes));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException cnfe) {
            IOException newE = new IOException(cnfe.getMessage());
            newE.initCause(cnfe);
            throw newE;
        }
    }

    // package level access because we don't want LoadFunc implementations
    // to get this information - this is to be used only from
    // MergeJoinIndexer
    int getSplitIndex() {
        return splitIndex;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return conf;
    }


    /** (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     * 
     * This will be called by 
     * {@link PigInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)}
     * to be used in {@link #write(DataOutput)} for serializing the 
     * wrappedSplit
     * 
     * This will be called by Hadoop in the backend to set the right Job 
     * Configuration (hadoop will invoke this method because PigSplit implements
     * {@link Configurable} - we need this Configuration in readFields() to
     * deserialize the wrappedSplit 
     */
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;        
    }

    // package level access because we don't want LoadFunc implementations
    // to get this information - this is to be used only from
    // PigInputFormat
    int getInputIndex() {
        return inputIndex;
    }

    /**
     * @return the totalSplits
     * package level access because we don't want LoadFunc implementations
     * to get this information - this is to be used only from
     * PigInputFormat
     */
    int getTotalSplits() {
        return totalSplits;
    }

    /**
     * @param totalSplits the totalSplits to set
     * package level access because we don't want LoadFunc implementations
     * to get this information - this is to be used only from
     * PigInputFormat
     */
    void setTotalSplits(int totalSplits) {
        this.totalSplits = totalSplits;
    }

}
