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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tools.bzip2r.CBZip2InputStream;

public class PigInputFormat implements InputFormat<Text, TargetedTuple>,
        JobConfigurable {

    public static final Log LOG = LogFactory
            .getLog(PigInputFormat.class);

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    /**
     * Is the given filename splitable? Usually, true, but if the file is stream
     * compressed, it will not be.
     * 
     * <code>FileInputFormat</code> implementations can override this and
     * return <code>false</code> to ensure that individual input files are
     * never split-up so that {@link Mapper}s process entire files.
     * 
     * @param fs
     *            the file system that the file is on
     * @param filename
     *            the file name to check
     * @return is this file splitable?
     */
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return !filename.getName().endsWith(".gz");
    }

    /**
     * List input directories. Subclasses may override to, e.g., select only
     * files matching a regular expression.
     * 
     * @param job
     *            the job to list input paths for
     * @return array of Path objects
     * @throws IOException
     *             if zero items.
     */
    protected Path[] listPaths(JobConf job) throws IOException {
        Path[] dirs = FileInputFormat.getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        
        List<Path> result = new ArrayList<Path>();
        for (Path p : dirs) {
            FileSystem fs = p.getFileSystem(job);
            FileStatus[] matches = fs.globStatus(p, hiddenFileFilter);
            for (FileStatus match : matches) {
                result.add(fs.makeQualified(match.getPath()));
            }
        }

        return result.toArray(new Path[result.size()]);
    }

    public void validateInput(JobConf job) throws IOException {
        /*ArrayList<FileSpec> inputs = (ArrayList<FileSpec>) ObjectSerializer
                .deserialize(job.get("pig.inputs"));
        Path[] inputDirs = new Path[inputs.size()];
        int i = 0;
        for (FileSpec spec : inputs) {
            inputDirs[i++] = new Path(spec.getFileName());
        }

        if (inputDirs.length == 0) {
            throw new IOException("No input paths specified in input");
        }

        List<IOException> result = new ArrayList<IOException>();
        int totalFiles = 0;
        for (Path p : inputDirs) {
            FileSystem fs = p.getFileSystem(job);
            if (fs.exists(p)) {
                // make sure all paths are files to avoid exception
                // while generating splits
                for (Path subPath : fs.listPaths(p, hiddenFileFilter)) {
                    FileSystem subFS = subPath.getFileSystem(job);
                    if (!subFS.exists(subPath)) {
                        result.add(new IOException(
                                "Input path does not exist: " + subPath));
                    } else {
                        totalFiles++;
                    }
                }
            } else {
                Path[] paths = fs.globPaths(p, hiddenFileFilter);
                if (paths.length == 0) {
                    result.add(new IOException("Input Pattern " + p
                            + " matches 0 files"));
                } else {
                    // validate globbed paths
                    for (Path gPath : paths) {
                        FileSystem gPathFS = gPath.getFileSystem(job);
                        if (!gPathFS.exists(gPath)) {
                            result.add(new FileNotFoundException(
                                    "Input path doesnt exist : " + gPath));
                        }
                    }
                    totalFiles += paths.length;
                }
            }
        }
        if (!result.isEmpty()) {
            throw new InvalidInputException(result);
        }
        // send output to client.
        LOG.info("Total input paths to process : " + totalFiles);*/
    }
    
    /**
     * Creates input splits one per input and slices of it
     * per DFS block of the input file. Configures the PigSplit
     * and returns the list of PigSplits as an array
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        ArrayList<FileSpec> inputs = (ArrayList<FileSpec>) ObjectSerializer
                .deserialize(job.get("pig.inputs"));
        ArrayList<List<OperatorKey>> inpTargets = (ArrayList<List<OperatorKey>>) ObjectSerializer
                .deserialize(job.get("pig.inpTargets"));
        PigContext pigContext = (PigContext) ObjectSerializer.deserialize(job
                .get("pig.pigContext"));
        
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < inputs.size(); i++) {
            Path path = new Path(inputs.get(i).getFileName());
            FuncSpec parser = inputs.get(i).getFuncSpec();
            FileSystem fs = path.getFileSystem(job);

            fs.setWorkingDirectory(new Path("/user", job.getUser()));
            ArrayList<Path> paths = new ArrayList<Path>();
            // If you give a non-glob name, globPaths returns a single
            // element with just that name.
            
            FileStatus[] matches = fs.globStatus(path, hiddenFileFilter);
            List<Path> matchList = new ArrayList<Path>();
            for (FileStatus match : matches) {
                matchList.add(match.getPath());
            }
            Path[] globPaths = matchList.toArray(new Path[matchList.size()]);
            for (int m = 0; m < globPaths.length; m++)
                paths.add(globPaths[m]);
            // paths.add(path);
            for (int j = 0; j < paths.size(); j++) {
                Path fullPath = new Path(fs.getWorkingDirectory(), paths.get(j));
                if (fs.getFileStatus(fullPath).isDir()) {
                    FileStatus children[] = fs.listStatus(fullPath, hiddenFileFilter);
                    for (int k = 0; k < children.length; k++) {
                        paths.add(children[k].getPath());
                    }
                    continue;
                }
                long bs = fs.getFileStatus(fullPath).getBlockSize();
                long size = fs.getFileStatus(fullPath).getLen();
                long pos = 0;
                String name = paths.get(j).getName();
                if (name.endsWith(".gz")) {
                    // Anything that ends with a ".gz" we must process as a
                    // complete file
                    splits.add(new PigSplit(pigContext, fs, fullPath, parser.toString(),
                            inpTargets.get(i), 0, size));
                } else {
                    while (pos < size) {
                        if (pos + bs > size)
                            bs = size - pos;
                        splits.add(new PigSplit(pigContext, fs, fullPath,
                                parser.toString(), inpTargets.get(i), pos, bs));
                        pos += bs;
                    }
                }
            }
        }
        return splits.toArray(new PigSplit[splits.size()]);
    }

    public RecordReader<Text, TargetedTuple> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        PigRecordReader r = new PigRecordReader(job, (PigSplit) split,
                compressionCodecs);
        return r;
    }

    private CompressionCodecFactory compressionCodecs = null;

    static public String codecList;

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
        codecList = conf.get("io.compression.codecs", "none");
    }

    /**
     * The class that actually reads the data off
     * the filesystem configured in the split and
     * parses tuples out of it
     */
    public static class PigRecordReader implements
            RecordReader<Text, TargetedTuple> {

        private InputStream is;

        private FSDataInputStream fsis;
        
        private BufferedPositionedInputStream bis;

        private long end;

        private PigSplit split;

        LoadFunc loader;

        CompressionCodecFactory compressionFactory;

        JobConf job;
        
        /**
         * Uses the split to get information about the input
         * and creates the inputstream to which the load func
         * can bind to
         * @param job
         * @param split
         * @param compressionFactory
         * @throws IOException
         */
        PigRecordReader(JobConf job, PigSplit split,
                CompressionCodecFactory compressionFactory) throws IOException {
            this.split = split;
            this.job = job;
            this.compressionFactory = compressionFactory;
            loader = split.getLoadFunction();
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(job);
            fs.setWorkingDirectory(new Path("/user/" + job.getUser()));
            CompressionCodec codec = compressionFactory.getCodec(split
                    .getPath());
            long start = split.getStart();
            fsis = fs.open(split.getPath());
            fsis.seek(start);

            if (codec != null) {
                is = codec.createInputStream(fsis);
                end = Long.MAX_VALUE;

            } else {
                end = start + split.getLength();

                if (split.file.getName().endsWith(".bz")
                        || split.file.getName().endsWith(".bz2")) {
                    is = new CBZip2InputStream(fsis, 9);
                } else {
                    is = fsis;
                }
            }
            bis = new BufferedPositionedInputStream(is, start);
            loader.bindTo(split.getPath().toString(), bis, start, end);

        }

        public JobConf getJobConf() {
            return job;
        }
        
        /**
         * Reads the next tuple using the load func and
         * sets its target operators. The targeted tuple
         * will then attached to those operators when the
         * plan is executed
         */
        public boolean next(Text key, TargetedTuple value) throws IOException {
            Tuple t = loader.getNext();
            if (t == null) {
                return false;
            }

            key.set(split.getPath().getName());

            value.reference(t);
            value.setTargetOps(split.getTargetOps());
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

        public TargetedTuple createValue() {
            return new TargetedTuple();
        }

        public float getProgress() throws IOException {
            float progress = getPos() - split.getStart();
            float finish = split.getLength();
            return progress / finish;
        }
    }

}
