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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.PigSlice;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * Wraps a {@link Slice} in an {@link InputSplit} so it's usable by hadoop.
 */
public class SliceWrapper extends InputSplit implements Writable {

    private int index;
    private ExecType execType;
    private Slice wrapped;
    private transient FileSystem fs;// transient so it isn't serialized
    private transient Configuration lastConf;
    private ArrayList<OperatorKey> targetOps;
    
    // XXX hadoop 20 new API integration: get around a hadoop 20 bug 
    // by setting total number of splits to each split so that it can
    // be passed to the back-end. This value is needed 
    // by PoissonSampleLoader to compute the number of samples
    private int totalSplits;

    public SliceWrapper() {
        // for deserialization
    }

    public SliceWrapper(Slice slice, ExecType execType, int index, FileSystem fs, ArrayList<OperatorKey> targetOps) {
        this.wrapped = slice;
        this.execType = execType;
        this.index = index;
        this.fs = fs;
        this.targetOps = targetOps;
    }
        
    public int getIndex() {
        return index;
    }

    public void setTotalSplits(int t) {
        totalSplits = t;
    }
    
    public int getTotalSplits() {
        return totalSplits;        
    }
    
    @Override
    public long getLength() throws IOException {
        return wrapped.getLength();
    }

    @Override
    public String[] getLocations() throws IOException {
        if(wrapped instanceof PigSlice) {
            Set<String> locations = new HashSet<String>();
            for (String loc : wrapped.getLocations()) {
                Path path = new Path(loc);
                FileStatus status = fs.getFileStatus(path); 
                BlockLocation[] b = fs.getFileBlockLocations(status, wrapped.getStart(), wrapped.getLength());
                int total = 0;
                for (int i = 0; i < b.length; i++) {
                    total += b[i].getHosts().length;
                }
                for (int i = 0; i < b.length; i++) {
                    String hosts[] = b[i].getHosts();
                    for (int j = 0; j < hosts.length; j++) {
                        locations.add(hosts[j]);
                    }
                }
                
            }
            return locations.toArray(new String[locations.size()]);
        } else {
            return wrapped.getLocations();
        }       
    }

    public Configuration getJobConf() {
        return lastConf;
    }


    @SuppressWarnings("unchecked")
    public RecordReader<Text, Tuple> makeReader(Configuration conf) throws IOException {
        lastConf = conf;        
        DataStorage store = new HDataStorage(ConfigurationUtil.toProperties(conf));

        // if the execution is against Mapred DFS, set
        // working dir to /user/<userid>
        if(execType == ExecType.MAPREDUCE)
            store.setActiveContainer(store.asContainer("/user/" + conf.get("user.name")));
        PigContext.setPackageImportList((ArrayList<String>)ObjectSerializer.deserialize(conf.get("udf.import.list")));
        wrapped.init(store);                             
        
        // Mimic org.apache.hadoop.mapred.FileSplit if feasible...
        String[] locations = wrapped.getLocations();
        if (locations.length > 0) {
            conf.set("map.input.file", locations[0]);    
            conf.setLong("map.input.start", wrapped.getStart());   
            conf.setLong("map.input.length", wrapped.getLength());
        }
        
        return new TupleReader();
    }
    
    public class TupleReader extends RecordReader<Text, Tuple> {
        private Tuple current;

        TupleFactory tupFac = TupleFactory.getInstance();
        
        @Override
        public void close() throws IOException {
            wrapped.close();
        } 

        @Override
        public float getProgress() throws IOException {
            return wrapped.getProgress();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public Tuple getCurrentValue() throws IOException, InterruptedException {
            return current;
        }

        @Override
        public void initialize(InputSplit inputsplit, 
            TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            Tuple t = tupFac.newTuple();
            boolean result = wrapped.next(t);
            current = t;
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput is) throws IOException {
        execType = (ExecType) readObject(is);
        targetOps = (ArrayList<OperatorKey>) readObject(is);
        index = is.readInt();
        wrapped = (Slice) readObject(is);
        totalSplits = is.readInt();
    }

    private Object readObject(DataInput is) throws IOException {
        byte[] bytes = new byte[is.readInt()];
        is.readFully(bytes);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                bytes));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException cnfe) {
            int errCode = 2094;
            String msg = "Unable to deserialize object.";
            throw new ExecException(msg, errCode, PigException.BUG, cnfe);
        }
    }

    @Override
    public void write(DataOutput os) throws IOException {
        writeObject(execType, os);
        writeObject(targetOps, os);
        os.writeInt(index);
        writeObject(wrapped, os);
        os.writeInt(totalSplits);
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

    /**
     * @return the wrapped Slice
     */
    public Slice getWrapped() {
        return wrapped;
    }

    public List<OperatorKey> getTargetOperatorKeyList() {
        return targetOps;
    }
}
