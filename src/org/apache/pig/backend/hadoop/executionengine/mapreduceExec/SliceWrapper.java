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

package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;

/**
 * Wraps a {@link Slice} in an {@link InputSplit} so it's usable by hadoop.
 */
public class SliceWrapper implements InputSplit {

    private EvalSpec groupbySpec;
    private EvalSpec evalSpec;
    private int index;
    private PigContext pigContext;
    private Slice wrapped;
    private transient FileSystem fs;// transient so it isn't serialized
    private transient JobConf lastConf;

    public SliceWrapper() {
        // for deserialization
    }

    public SliceWrapper(Slice slice, PigContext context, EvalSpec groupbySpec,
            EvalSpec evalSpec, int index, FileSystem fs) {
        this.wrapped = slice;
        this.pigContext = context;
        this.groupbySpec = groupbySpec;
        this.evalSpec = evalSpec;
        this.index = index;
        this.fs = fs;
    }

    public EvalSpec getEvalSpec() {
        return evalSpec;
    }

    public EvalSpec getGroupbySpec() {
        return groupbySpec;
    }

    public int getIndex() {
        return index;
    }

    public long getLength() throws IOException {
        return wrapped.getLength();
    }

    public String[] getLocations() throws IOException {
        Set<String> locations = new HashSet<String>();
        for (String loc : wrapped.getLocations()) {
            Path path = new Path(loc);
            BlockLocation[] blocks = fs.getFileBlockLocations(path, 0, fs.getFileStatus(
                                            path).getLen());
            for (int i = 0; i < blocks.length; i++) {
                String[] hosts = blocks[i].getHosts();
                for (int j = 0; j < hosts.length; j++){
                    locations.add(hosts[j]);
                }
            }
        }
        return locations.toArray(new String[locations.size()]);
    }

    public JobConf getJobConf() {
        return lastConf;
    }

    public RecordReader<Text, Tuple> makeReader(JobConf job) throws IOException {
        lastConf = job;        
        DataStorage store = new HDataStorage(ConfigurationUtil.toProperties(job));
        store.setActiveContainer(store.asContainer("/user/" + job.getUser()));
        wrapped.init(store);
        
        // Mimic org.apache.hadoop.mapred.FileSplit if feasible...
        String[] locations = wrapped.getLocations();
        if (locations.length > 0) {
            job.set("map.input.file", locations[0]);    
            job.setLong("map.input.start", wrapped.getStart());   
            job.setLong("map.input.length", wrapped.getLength());
        }
        
        return new RecordReader<Text, Tuple>() {

            public void close() throws IOException {
                wrapped.close();
            }

            public Text createKey() {
                return new Text();
            }

            public Tuple createValue() {
                return new Tuple();
            }

            public long getPos() throws IOException {
                return wrapped.getPos();
            }

            public float getProgress() throws IOException {
                return wrapped.getProgress();
            }

            public boolean next(Text key, Tuple value) throws IOException {
                return wrapped.next(value);
            }
        };
    }

    public void readFields(DataInput is) throws IOException {
        pigContext = (PigContext) readObject(is);

        groupbySpec = (EvalSpec) readObject(is);
        if (groupbySpec != null) {
            groupbySpec.instantiateFunc(pigContext);
        }
        evalSpec = (EvalSpec) readObject(is);
        if (evalSpec != null) {
            evalSpec.instantiateFunc(pigContext);
        }
        index = is.readInt();
        wrapped = (Slice) readObject(is);
    }

    private IOException wrapException(Exception e) {
        IOException newE = new IOException(e.getMessage());
        newE.initCause(e);
        return newE;
    }

    private Object readObject(DataInput is) throws IOException {
        byte[] bytes = new byte[is.readInt()];
        is.readFully(bytes);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                bytes));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException cnfe) {
            IOException newE = wrapException(cnfe);
            throw newE;
        }
    }

    public void write(DataOutput os) throws IOException {
        writeObject(pigContext, os);
        writeObject(groupbySpec, os);
        writeObject(evalSpec, os);
        os.writeInt(index);
        writeObject(wrapped, os);
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

}
