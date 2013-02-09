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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
 */
public class Bloom extends FilterFunc {

    private String bloomFile;
    public BloomFilter filter = null;

    /** 
     * @param filename file containing the serialized Bloom filter
     */
    public Bloom(String filename) {
        bloomFile = filename;
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (filter == null) {
            init();
        }
        byte[] b;
        if (input.size() == 1) b = DataType.toBytes(input.get(0));
        else b = DataType.toBytes(input, DataType.TUPLE);

        Key k = new Key(b);
        return filter.membershipTest(k);
    }

    @Override
    public List<String> getCacheFiles() {
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

    private void init() throws IOException {
        filter = new BloomFilter();
        String dcFile = "./" + getFilenameFromPath(bloomFile) +
            "/part-r-00000";
        filter.readFields(new DataInputStream(new FileInputStream(dcFile)));
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
        return p.replace("/", "_");
    }

}
