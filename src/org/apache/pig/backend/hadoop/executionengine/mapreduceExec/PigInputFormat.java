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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tools.bzip2r.CBZip2InputStream;


public class PigInputFormat implements InputFormat<Text, Tuple>, JobConfigurable {

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        
        ArrayList<FileSpec> inputs = (ArrayList<FileSpec>) ObjectSerializer.deserialize(job.get("pig.inputs"));        
        ArrayList<EvalSpec> mapFuncs = (ArrayList<EvalSpec>) ObjectSerializer.deserialize(job.get("pig.mapFuncs",""));        
        ArrayList<EvalSpec> groupFuncs = (ArrayList<EvalSpec>) ObjectSerializer.deserialize(job.get("pig.groupFuncs", ""));        
        PigContext pigContext = (PigContext)ObjectSerializer.deserialize(job.get("pig.pigContext"));
        //TODO: don't understand this code
        // added for UNION: set group func arity to match arity of inputs
        if (groupFuncs!=null && groupFuncs.size() != inputs.size()) {
            groupFuncs = new ArrayList<EvalSpec>();
            for (int i = 0; i < groupFuncs.size(); i++) {
                groupFuncs.set(i,null);
            }
        }
        
        if (inputs.size() != mapFuncs.size()) {
            throw new IOException("number of inputs != number of map functions: " + inputs.size() + " != "
                    + mapFuncs.size() + ": " + job.get("pig.mapFuncs", "missing"));
        }
        
        if (groupFuncs!= null && inputs.size() != groupFuncs.size()) {
            throw new IOException("number of inputs != number of group functions: " + inputs.size() + " != "
                    + groupFuncs.size());
        }
        
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < inputs.size(); i++) {
            Path path = new Path(inputs.get(i).getFileName());
            String parser = inputs.get(i).getFuncSpec();
            FileSystem fs = path.getFileSystem(job);

            fs.setWorkingDirectory(new Path("/user", job.getUser()));
            ArrayList<Path> paths = new ArrayList<Path>();
            // If you give a non-glob name, globPaths returns a single
            // element with just that name.
            Path[] globPaths = fs.globPaths(path); 
            for (int m = 0; m < globPaths.length; m++) paths.add(globPaths[m]);
            //paths.add(path);
            for (int j = 0; j < paths.size(); j++) {
                Path fullPath = new Path(fs.getWorkingDirectory(), paths.get(j));
                if (fs.getFileStatus(fullPath).isDir()) {
                    FileStatus children[] = fs.listStatus(fullPath);
                    for(int k = 0; k < children.length; k++) {
                        paths.add(children[k].getPath());
                    }
                    continue;
                }
                long bs = fs.getFileStatus(fullPath).getBlockSize();
                long size = fs.getFileStatus(fullPath).getLen();
                long pos = 0;
                String name = paths.get(j).getName();
                if (name.endsWith(".gz")) {
                    // Anything that ends with a ".gz" we must process as a complete file
                    splits.add(new PigSplit(pigContext, fs, fullPath, parser, groupFuncs==null ? null : groupFuncs.get(i), mapFuncs.get(i), i, 0, size));
                } else {
                    while (pos < size) {
                        if (pos + bs > size)
                            bs = size - pos;
                        splits.add(new PigSplit(pigContext, fs, fullPath, parser, groupFuncs==null ? null : groupFuncs.get(i), mapFuncs.get(i), i, pos, bs));
                        pos += bs;
                    }
                }
            }
        }
        return splits.toArray(new PigSplit[splits.size()]);
    }

   public RecordReader<Text, Tuple> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        PigRecordReader r = new PigRecordReader(job, (PigSplit)split, compressionCodecs);
        return r;
    }

    private CompressionCodecFactory compressionCodecs = null;

    static public String codecList;
    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
        codecList = conf.get("io.compression.codecs", "none");
    }
    
    public static class PigRecordReader implements RecordReader<Text, Tuple> {
        /**
         * This is a tremendously ugly hack to get around the fact that mappers do not have access
         * to their readers. We take advantage of the fact that RecordReader.next and Mapper.map is
         * run on same the thread to share information through a thread local variable.
         */
        static ThreadLocal<PigRecordReader> myReader = new ThreadLocal<PigRecordReader>();

        public static PigRecordReader getPigRecordReader() {
            return myReader.get();
        }

        private InputStream     is;
        private FSDataInputStream   fsis;
        private long            end;
        private PigSplit    split;
        LoadFunc                loader;
        CompressionCodecFactory compressionFactory;
        JobConf job;
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        
        PigRecordReader(JobConf job, PigSplit split, CompressionCodecFactory compressionFactory) throws IOException {
            this.split = split;
            this.job = job;
            this.compressionFactory = compressionFactory;
            loader = split.getLoadFunction();
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(job);
            fs.setWorkingDirectory(new Path("/user/" + job.getUser()));
            CompressionCodec codec = compressionFactory.getCodec(split.getPath());
            long start = split.getStart();
            fsis = fs.open(split.getPath());
            fsis.seek(start);

            if (codec != null) {
                is = codec.createInputStream(fsis);
                end = Long.MAX_VALUE;
                
            } else{
                end = start + split.getLength();    
                
                if (split.file.getName().endsWith(".bz") ||
                        split.file.getName().endsWith(".bz2")) {
                    is = new CBZip2InputStream(fsis,9);
                }else{
                    is = fsis;
                }
            }
            myReader.set(this);
            loader.bindTo(split.getPath().toString(), new BufferedPositionedInputStream(is, start), start, end);
            
        }

        public JobConf getJobConf(){
            return job;
        }

        public boolean next(Text key, Tuple value) throws IOException {
            Tuple t = loader.getNext();
            if (t == null) {
                return false;
            }

            key.set(split.getPath().getName());
            value.reference(t);
            return true;
        }

        public long getPos() throws IOException {
            return fsis.getPos();
        }

        public void close() throws IOException {
            is.close();
        }

        public PigSplit getPigFileSplit() {
            return split;
        }

        public Text createKey() {
            return new Text();
        }

        public Tuple createValue() {
            return mTupleFactory.newTuple();
        }

    public float getProgress() throws IOException {
        float progress = getPos() - split.getStart();
        float finish = split.getLength();
        return progress/finish;
    }
    }

    public void validateInput(JobConf arg0) throws IOException {
    }

 }
