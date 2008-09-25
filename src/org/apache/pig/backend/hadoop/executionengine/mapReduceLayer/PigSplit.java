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
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * The main split class that maintains important
 * information about the input split.
 *
 */
public class PigSplit implements InputSplit {
    //The input file this split represents
    Path file;
    
    //The start pos from which this split starts
    //in file
    long start;

    //The length of this split
    long length;

    //The operators to which the tuples from this
    //input file are attached. These are the successors
    //of the load operator representing this input
    ArrayList<OperatorKey> targetOps;

    //The load function that parses this split and
    //produces tuples from it
    String parser;

    //The filesystem on which file resides
    FileSystem fs;

    //The context in which Pig was running when this
    //job got submitted.
    PigContext pigContext;

    public PigSplit() {
    }

    public PigSplit(PigContext pigContext, FileSystem fs, Path path,
            String parser, List<OperatorKey> targetOps, long start, long length) {
        this.fs = fs;
        this.file = path;
        this.start = start;
        this.length = length;
        this.targetOps = (ArrayList) targetOps;
        this.parser = parser;
        this.pigContext = pigContext;
    }

    public String getParser() {
        return parser;
    }

    public long getStart() {
        return start;
    }

    public long getLength() {
        return length;
    }

    public Path getPath() {
        return file;
    }

    public List<OperatorKey> getTargetOps() {
        return targetOps;
    }

    public LoadFunc getLoadFunction() {
        LoadFunc loader = null;
        if (this.parser == null) {
            loader = new PigStorage();
        } else {
            try {
                loader = (LoadFunc) PigContext
                        .instantiateFuncFromSpec(new FuncSpec(this.parser));
            } catch (Exception exp) {
                throw new RuntimeException("can't instantiate " + parser);
            }
        }
        return loader;
    }

    public String[] getLocations() throws IOException {
        FileStatus status = fs.getFileStatus(file);
        BlockLocation[] b = fs.getFileBlockLocations(status, start, length);
        int total = 0;
        for (int i = 0; i < b.length; i++) {
            total += b[i].getHosts().length;
        }
        String locations[] = new String[total];
        int count = 0;
        for (int i = 0; i < b.length; i++) {
            String hosts[] = b[i].getHosts();
            for (int j = 0; j < hosts.length; j++) {
                locations[count++] = hosts[j];
            }
        }
        return locations;
    }

    public void readFields(DataInput is) throws IOException {
        file = new Path(is.readUTF());
        start = is.readLong();
        length = is.readLong();
        pigContext = (PigContext) readObject(is);
        targetOps = (ArrayList<OperatorKey>) readObject(is);
        parser = is.readUTF();
    }

    public void write(DataOutput os) throws IOException {
        os.writeUTF(file.toString());
        os.writeLong(start);
        os.writeLong(length);
        writeObject(pigContext, os);
        writeObject(targetOps, os);
        os.writeUTF(parser);
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

}
