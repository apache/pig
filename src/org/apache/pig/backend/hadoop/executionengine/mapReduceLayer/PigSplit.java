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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.PigConfiguration;
import org.apache.pig.data.WritableByteArray;
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


    private static String FILESPLIT_CLASSNAME = FileSplit.class.getName();
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
    private InputSplit[] wrappedSplits;

    // index of the wrappedSplit in the list of splits returned by
    // InputFormat.getSplits()
    // This will be used by MergeJoinIndexer to record the split # in the
    // index
    private int splitIndex;

    // index of current splits being process
    private int currentIdx;

    // the flag indicates this is a multi-input join (i.e. join)
    // so that custom Hadoop counters will be created in the
    // back-end to track the number of records for each input.
    private boolean isMultiInputs = false;

    // the flag indicates the custom Hadoop counter should be disabled.
    // This is to prevent the number of counters exceeding the limit.
    // This flag is controlled by Pig property "pig.disable.counter" (
    // the default value is 'false').
    private boolean disableCounter = false;

    /**
     * the job Configuration
     */
    private Configuration conf;

    /**
     * total number of splits - required by skew join
     */
    private int totalSplits;

    /**
     * total length
     */
    private long length = -1;

    /**
     * overall locations
     */
    String[] locations = null;


    /**
     * overall splitLocationInfos
     */
    SplitLocationInfo[] splitLocationInfos = null;

    // this seems necessary for Hadoop to instatiate this split on the
    // backend
    public PigSplit() {}

    public PigSplit(InputSplit[] wrappedSplits, int inputIndex,
            List<OperatorKey> targetOps, int splitIndex) {
        this.wrappedSplits = wrappedSplits;
        this.inputIndex = inputIndex;
        this.targetOps = new ArrayList<OperatorKey>(targetOps);
        this.splitIndex = splitIndex;
        this.currentIdx = 0;
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
        return wrappedSplits[currentIdx];
    }

    /**
     *
     * @param idx the index into the wrapped splits
     * @return the specified wrapped split
     */
    public InputSplit getWrappedSplit(int idx) {
        return wrappedSplits[idx];
    }

    @Override
    @SuppressWarnings("unchecked")
    public String[] getLocations() throws IOException, InterruptedException {
        if (locations == null) {
            HashMap<String, Long> locMap = new HashMap<String, Long>();
            Long lenInMap;
            for (InputSplit split : wrappedSplits)
            {
                String[] locs = split.getLocations();
                for (String loc : locs)
                {
                    if ((lenInMap = locMap.get(loc)) == null)
                        locMap.put(loc, split.getLength());
                    else
                        locMap.put(loc, lenInMap + split.getLength());
                }
            }
            Set<Map.Entry<String, Long>> entrySet = locMap.entrySet();
            Map.Entry<String, Long>[] hostSize =
                entrySet.toArray(new Map.Entry[entrySet.size()]);
            Arrays.sort(hostSize, new Comparator<Map.Entry<String, Long>>() {

              @Override
              public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
                long diff = o1.getValue() - o2.getValue();
                if (diff < 0) return 1;
                if (diff > 0) return -1;
                return 0;
              }
            });
            // maximum 5 locations are in list: refer to PIG-1648 for more details
            int nHost = Math.min(hostSize.length, 5);
            locations = new String[nHost];
            for (int i = 0; i < nHost; ++i) {
              locations[i] = hostSize[i].getKey();
            }
        }
        return locations;
    }


    @Override
    public SplitLocationInfo[] getLocationInfo() throws IOException {
        if (splitLocationInfos == null) {
            HashMap<SplitLocationInfo, Long> locMap = new HashMap<SplitLocationInfo, Long>();
            Long lenInMap;
            for (InputSplit split : wrappedSplits) {
                SplitLocationInfo[] locs = split.getLocationInfo();
                if( locs != null) {
                    for (SplitLocationInfo loc : locs) {
                        try {
                            if ((lenInMap = locMap.get(loc)) == null)
                                locMap.put(loc, split.getLength());
                            else
                                locMap.put(loc, lenInMap + split.getLength());
                        } catch (InterruptedException e) {
                            throw new RuntimeException("InputSplit.getLength throws exception: ", e);
                        }
                    }
                }
            }
            Set<Map.Entry<SplitLocationInfo, Long>> entrySet = locMap.entrySet();
            Map.Entry<SplitLocationInfo, Long>[] hostSize =
                    entrySet.toArray(new Map.Entry[entrySet.size()]);
            Arrays.sort(hostSize, new Comparator<Map.Entry<SplitLocationInfo, Long>>() {

                @Override
                public int compare(Entry<SplitLocationInfo, Long> o1, Entry<SplitLocationInfo, Long> o2) {
                    long diff = o1.getValue() - o2.getValue();
                    if (diff < 0) return 1;
                    if (diff > 0) return -1;
                    return 0;
                }
            });
            // maximum 5 locations are in list: refer to PIG-1648 for more details
            int nHost = Math.min(hostSize.length, 5);
            splitLocationInfos = new SplitLocationInfo[nHost];
            for (int i = 0; i < nHost; ++i) {
                splitLocationInfos[i] = hostSize[i].getKey();
            }
        }
        return splitLocationInfos;
    }


    @Override
    public long getLength() throws IOException, InterruptedException {
        if (length == -1) {
            length = 0;
            for (int i = 0; i < wrappedSplits.length; i++)
                length += wrappedSplits[i].getLength();
        }
        return length;
    }

    /**
     * Return the length of a wrapped split
     * @param idx the index into the wrapped splits
     * @return number of wrapped splits
     */
    public long getLength(int idx) throws IOException, InterruptedException {
        return wrappedSplits[idx].getLength();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput is) throws IOException {
        disableCounter = is.readBoolean();
        isMultiInputs = is.readBoolean();
        totalSplits = is.readInt();
        splitIndex = is.readInt();
        inputIndex = is.readInt();
        targetOps = (ArrayList<OperatorKey>) readObject(is);
        int splitLen = is.readInt();
        int distinctSplitClassCount = is.readInt();
        boolean nonFileSplit = false;
        //construct the input split class name list

        String[] distinctSplitClassName = new String[distinctSplitClassCount];
        for (int i = 0; i < distinctSplitClassCount; i++) {
            distinctSplitClassName[i] = is.readUTF();
            if (!distinctSplitClassName[i].equals(FILESPLIT_CLASSNAME)) {
                nonFileSplit = true;
            }
        }
        try {
            SerializationFactory sf = new SerializationFactory(conf);
            // The correct call sequence for Deserializer is, we shall open, then deserialize, but we shall not close
            wrappedSplits = new InputSplit[splitLen];

            if (splitLen <= 0) {
                return;
            }

            // Do not compress if everything is FileSplit as it does not compress much
            // but adds few seconds for 30K+ tasks
            boolean compress = nonFileSplit && conf.getBoolean(
                    PigConfiguration.PIG_COMPRESS_INPUT_SPLITS,
                    PigConfiguration.PIG_COMPRESS_INPUT_SPLITS_DEFAULT);
            DataInputStream dis = null;
            if (compress) {
                int numBytes = is.readInt();
                byte[] buf = new byte[numBytes];
                is.readFully(buf, 0, numBytes);
                dis = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(buf)));
            }
            DataInput dataIn = compress ? dis : is;
            for (int i = 0; i < splitLen; i++)
            {
                //read the className index
                int index = dataIn.readInt();
                //get the split class name
                String splitClassName = distinctSplitClassName[index];
                Class splitClass = conf.getClassByName(splitClassName);
                Deserializer d = sf.getDeserializer(splitClass);
                d.open((InputStream) dataIn);
                wrappedSplits[i] = (InputSplit)ReflectionUtils.newInstance(splitClass, conf);
                d.deserialize(wrappedSplits[i]);
            }
            if (compress && splitLen > 0) {
                dis.close();
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(DataOutput os) throws IOException {
        os.writeBoolean(disableCounter);
        os.writeBoolean(isMultiInputs);
        os.writeInt(totalSplits);
        os.writeInt(splitIndex);
        os.writeInt(inputIndex);
        writeObject(targetOps, os);
        os.writeInt(wrappedSplits.length);
        Set<String> splitClassNameSet = new HashSet<String>();
        //first get the distinct split class name set
        for ( int i= 0; i < wrappedSplits.length; i++) {
            splitClassNameSet.add(wrappedSplits[i].getClass().getName());
        }
        List<String> distinctSplitClassList = new ArrayList<String>();
        distinctSplitClassList.addAll(splitClassNameSet);
        boolean nonFileSplit = distinctSplitClassList.size() > 1 || (!distinctSplitClassList.contains(FILESPLIT_CLASSNAME));
        //write the distinct number of split class name
        os.writeInt(distinctSplitClassList.size());
        //write each classname once
        for (int i = 0 ; i < distinctSplitClassList.size(); i++) {
            os.writeUTF(distinctSplitClassList.get(i));
        }
        SerializationFactory sf = new SerializationFactory(conf);

        if (wrappedSplits.length <= 0) {
            return;
        }

        boolean compress = nonFileSplit && conf.getBoolean(
                PigConfiguration.PIG_COMPRESS_INPUT_SPLITS,
                PigConfiguration.PIG_COMPRESS_INPUT_SPLITS_DEFAULT);
        WritableByteArray byteStream = null;
        Deflater deflater = null;
        DataOutputStream dos = null;
        if (compress) {
            byteStream = new WritableByteArray(16384);
            deflater = new Deflater(Deflater.BEST_COMPRESSION);
            dos = new DataOutputStream(new DeflaterOutputStream(byteStream, deflater));
        }
        DataOutput dataOut = compress ? dos : os;
        for (int i = 0; i < wrappedSplits.length; i++)
        {
            //find out the index of the split class name
            int index = distinctSplitClassList.indexOf(wrappedSplits[i].getClass().getName());
            dataOut.writeInt(index);
            Serializer s = sf.getSerializer(wrappedSplits[i].getClass());
            //Checks if Serializer is NULL or not before calling open() method on it.
            if (s == null) {
                throw new IllegalArgumentException("Could not find Serializer for class "+wrappedSplits[i].getClass()+". InputSplits must implement Writable.");
            }
            s.open((OutputStream) dataOut);
            // The correct call sequence for Serializer is, we shall open, then serialize, but we shall not close
            s.serialize(wrappedSplits[i]);
        }
        if (compress) {
            //Get the compressed serialized bytes and write them
            dos.close();
            os.writeInt(byteStream.getLength());
            os.write(byteStream.getData(), 0, byteStream.getLength());
            deflater.end();
        }

    }

    private void writeObject(Serializable obj, DataOutput os)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
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

    public int getSplitIndex() {
        return splitIndex;
    }

    /**
     * Indicates this map has multiple input (such as the result of
     * a join operation).
     * @param b true if the map has multiple inputs
     */
    public void setMultiInputs(boolean b) {
        isMultiInputs = b;
    }

    /**
     * Returns true if the map has multiple inputs, else false
     * @return true if the map has multiple inputs, else false
     */
    public boolean isMultiInputs() {
        return isMultiInputs;
    }

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
     *
     * @return the number of wrapped splits
     */
    public int getNumPaths() {
        return wrappedSplits.length;
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

    @Override
    public String toString() {
        StringBuilder st = new StringBuilder();
        st.append("Number of splits :" + wrappedSplits.length+"\n");
        try {
            st.append("Total Length = "+ getLength()+"\n");
            for (int i = 0; i < wrappedSplits.length; i++) {
                st.append("Input split["+i+"]:\n   Length = "+ wrappedSplits[i].getLength()+"\n   ClassName: " +
                    wrappedSplits[i].getClass().getName() + "\n   Locations:\n");
                for (String location :  wrappedSplits[i].getLocations())
                    st.append("    "+location+"\n");
                st.append("\n-----------------------\n");
          }
        } catch (IOException e) {
          return null;
        } catch (InterruptedException e) {
          return null;
        }
        return st.toString();
    }

    public void setDisableCounter(boolean disableCounter) {
        this.disableCounter = disableCounter;
    }

    public boolean disableCounter() {
        return disableCounter;
    }

    public void setCurrentIdx(int idx) {
        this.currentIdx = idx;
    }
}
