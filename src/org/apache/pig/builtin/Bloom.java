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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Use a Bloom filter build previously by BuildBloom.  You would first
 * build a bloom filter in a group all job.  For example:
 * in a group all job.  For example:
 * define bb BuildBloom('jenkins', '100', '0.1');
 * A = load 'foo' as (x, y);
 * B = group A all;
 * C = foreach B generate bb(A.x);
 * store C into 'mybloom';
 * The bloom filter can be on multiple keys by passing more than one field
 * (or the entire bag) to BuildBloom.
 * The resulting file can then be used in a Bloom filter as:
 * define bloom Bloom(mybloom);
 * A = load 'foo' as (x, y);
 * B = load 'bar' as (z);
 * C = filter B by bloom(z);
 * D = join C by z, A by x;
 * It uses {@link org.apache.hadoop.util.bloom.BloomFilter}.
 *
 * You can also pass the Bloom filter from BuildBloom directly to Bloom UDF
 * as a scalar instead of storing it to file and loading again. This is simpler
 * if the Bloom filter will not be reused and needs to be discarded after the
 * run of the script.
 *
 * define bb BuildBloom('jenkins', '100', '0.1');
 * A = load 'foo' as (x, y);
 * B = group A all;
 * C = foreach B generate bb(A.x) as bloomfilter;
 * D = load 'bar' as (z);
 * E = filter D by Bloom(C.bloomfilter, z);
 * F = join E by z, A by x;
 */
public class Bloom extends FilterFunc {

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    private String bloomFile;
    private BloomFilter filter = null;

    public Bloom() {
    }

    /**
     * The filename containing the serialized Bloom filter. If filename is null
     * or the no-arg constructor is used, then the bloomfilter bytearray which
     * is the output of BuildBloom should be passed as the first argument to the UDF
     *
     * @param filename  file containing the serialized Bloom filter
     */
    public Bloom(String filename) {
        bloomFile = filename;
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (filter == null) {
            init(input);
        }
        byte[] b;
        if (bloomFile == null) {
            // The first one is the bloom filter. Skip that
            if (input.size() == 2) {
                b = DataType.toBytes(input.get(1));
            } else {
                List<Object> inputList = input.getAll();
                Tuple tuple = mTupleFactory.newTupleNoCopy(inputList.subList(1, inputList.size()));
                b = DataType.toBytes(tuple, DataType.TUPLE);
            }
        } else {
            if (input.size() == 1) {
                b = DataType.toBytes(input.get(0));
            } else {
                b = DataType.toBytes(input, DataType.TUPLE);
            }
        }

        Key k = new Key(b);
        return filter.membershipTest(k);
    }

    @Override
    public List<String> getCacheFiles() {
        if (bloomFile != null) {
            List<String> list = new ArrayList<String>(1);
            // We were passed the name of the file on HDFS.  Append a
            // name for the file on the task node.
            try {
                list.add(bloomFile + "#" + getFilenameFromPath(bloomFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return list;
        }
        return null;
    }

    private void init(Tuple input) throws IOException {
        if (bloomFile == null) {
            if (input.get(0) instanceof DataByteArray) {
                filter = BuildBloomBase.bloomIn((DataByteArray) input.get(0));
            } else {
                throw new IllegalArgumentException("The first argument to the Bloom UDF should be"
                        + " the bloom filter if a bloom file is not specified in the constructor");
            }
        } else {
            filter = new BloomFilter();
            String dir = "./" + getFilenameFromPath(bloomFile);
            String[] partFiles = new File(dir)
                    .list(new FilenameFilter() {
                        @Override
                        public boolean accept(File current, String name) {
                            return name.startsWith("part");
                        }
                    });

            String dcFile = dir + "/" + partFiles[0];
            DataInputStream dis = new DataInputStream(new FileInputStream(dcFile));
            try {
                filter.readFields(dis);
            } finally {
                dis.close();
            }
        }
    }

    /**
     * For testing only, do not use directly.
     */
    public void setFilter(DataByteArray dba) throws IOException {
        DataInputStream dis = new DataInputStream(new
            ByteArrayInputStream(dba.get()));
        filter = new BloomFilter();
        filter.readFields(dis);
    }

    private String getFilenameFromPath(String p) throws IOException {
        Path path = new Path(p);
        return path.toUri().getPath().replace("/", "_");
    }

}
