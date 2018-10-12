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

package org.apache.pig.impl.bloom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.org.roaringbitmap.RoaringBitmap;

public class BloomFilter extends Filter {
    private static final Log LOG = LogFactory.getLog(BloomFilter.class);
    private static final int VERSION = 1;
    private HashFunction hash;
    private RoaringBitmap bitmap;
    private int hashAlgorithm;

    /**
     * Read the fields using bloomIn
     */
    public BloomFilter() {
        super();
    }

    public BloomFilter(int vectorSize, int nbHash, int hashAlgorithm) {
        super.vectorSize = vectorSize;
        super.nbHash = nbHash;
        this.hashAlgorithm = hashAlgorithm;
        this.hash = new HashFunction(vectorSize, nbHash, hashAlgorithm);
        this.bitmap = new RoaringBitmap();
    }

    @Override
    public void add(Key key) {
        if(key == null) {
            throw new NullPointerException("key cannot be null");
        }
        int[] h = hash.hash(key);
        hash.clear();
        Arrays.sort(h);
        this.bitmap.or(RoaringBitmap.bitmapOf(h));
    }

    @Override
    public boolean membershipTest(Key key) {
        if(key == null) {
            throw new NullPointerException("key cannot be null");
        }
        int[] h = hash.hash(key);
        hash.clear();
        for(int i = 0; i < nbHash; i++) {
            if(!this.bitmap.contains(h[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void and(Filter filter) {
        if(filter == null
                || !(filter instanceof BloomFilter)
                || ((BloomFilter)filter).vectorSize != this.vectorSize
                || ((BloomFilter)filter).nbHash != this.nbHash) {
              throw new IllegalArgumentException("filters cannot be and-ed");
        }
        this.bitmap.and(((BloomFilter)filter).bitmap);
    }

    @Override
    public void or(Filter filter) {
        if(filter == null
                || !(filter instanceof BloomFilter)
                || ((BloomFilter)filter).vectorSize != this.vectorSize
                || ((BloomFilter)filter).nbHash != this.nbHash) {
              throw new IllegalArgumentException("filters cannot be or-ed");
        }
        this.bitmap.or(((BloomFilter)filter).bitmap);
    }

    @Override
    public void xor(Filter filter) {
        if(filter == null
                || !(filter instanceof BloomFilter)
                || ((BloomFilter)filter).vectorSize != this.vectorSize
                || ((BloomFilter)filter).nbHash != this.nbHash) {
              throw new IllegalArgumentException("filters cannot be xor-ed");
        }
        this.bitmap.xor(((BloomFilter)filter).bitmap);
    }

    @Override
    public void not() {
        this.bitmap.flip(0, vectorSize);
    }

    @Override
    public String toString() {
      return this.bitmap.toString();
    }

    /**
     * @return size of the the bloomfilter
     */
    public int getVectorSize() {
      return this.vectorSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(VERSION);
        out.writeInt(this.nbHash);
        out.writeByte(this.hashAlgorithm);
        out.writeInt(this.vectorSize);
        this.bitmap.runOptimize();
        ByteArrayOutputStream bos = compressBitmap();
        LOG.info("Compressed bitmap from " + String.format("%,8d bytes", this.bitmap.getSizeInBytes())
        + " to "+ String.format("%,8d bytes", bos.size()));
        out.writeInt(bos.size());
        out.write(bos.toByteArray());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int ver = in.readInt();
        if (ver == VERSION) {
            this.nbHash = in.readInt();
            this.hashAlgorithm = in.readByte();
        } else {
            throw new IOException("Unsupported version: " + ver);
        }
        this.vectorSize = in.readInt();
        this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashAlgorithm);
        this.bitmap = new RoaringBitmap();
        int compressedSize = in.readInt();
        byte[] buf = new byte[compressedSize];
        in.readFully(buf);
        this.bitmap.deserialize(decompressBitmap(buf));
    }

    public static BloomFilter bloomIn(DataByteArray b) throws IOException {
        DataInputStream dis = new DataInputStream(new
            ByteArrayInputStream(b.get()));
        BloomFilter f = new BloomFilter();
        f.readFields(dis);
        return f;
    }
    private ByteArrayOutputStream compressBitmap() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BZip2Codec bzip = new BZip2Codec();
        bzip.setConf(new Configuration(false));
        CompressionOutputStream compressionOut = bzip.createOutputStream(bos);
        DataOutputStream dos = new DataOutputStream(compressionOut);
        this.bitmap.serialize(dos);
        compressionOut.finish();
        dos.flush();
        return bos;
    }

    private DataInput decompressBitmap(byte[] buffer) throws IOException {
        ByteArrayInputStream deCompressedDataBuffer = new ByteArrayInputStream(buffer, 0, buffer.length);
        BZip2Codec bzip = new BZip2Codec();
        bzip.setConf(new Configuration(false));
        CompressionInputStream compressionIn = bzip.createInputStream(deCompressedDataBuffer);
        DataInputStream inflateIn = new DataInputStream(compressionIn);
        return inflateIn;
    }
}
