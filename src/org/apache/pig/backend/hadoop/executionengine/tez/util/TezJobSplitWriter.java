/**
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
package org.apache.pig.backend.hadoop.executionengine.tez.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

public class TezJobSplitWriter {
    private static final Log LOG = LogFactory.getLog(TezJobSplitWriter.class);
    private static final int splitVersion = 1;
    private static final byte[] SPLIT_FILE_HEADER;
    static {
        try {
            SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException u) {
            throw new RuntimeException(u);
        }
    }
    static final byte[] META_SPLIT_FILE_HEADER;
    static {
        try {
            META_SPLIT_FILE_HEADER = "META-SPL".getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException u) {
            throw new RuntimeException(u);
        }
    }

    /**
     * Create split files and write splits as well as as splits metadata
     * @param jobSubmitDir
     * @param conf
     * @param fs
     * @param splits
     * @param splitsProto
     * @throws IOException
     * @throws InterruptedException
     */
    public static  <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, Configuration conf, FileSystem fs,
            T[] splits, MRSplitsProto splitsProto) throws IOException, InterruptedException {
        FSDataOutputStream out = createFile(fs, JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
        SplitMetaInfo[] info = writeSplits(conf, splits, out, splitsProto);
        out.close();
        writeJobSplitMetaInfo(fs, JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir),
                new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion, info);
    }

    private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, Configuration job) throws IOException {
        FSDataOutputStream out = FileSystem.create(fs, splitFile,
                new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
        int replication = job.getInt(Job.SUBMIT_REPLICATION, 10);
        fs.setReplication(splitFile, (short) replication);
        writeSplitHeader(out);
        return out;
    }

    private static void writeSplitHeader(FSDataOutputStream out) throws IOException {
        out.write(SPLIT_FILE_HEADER);
        out.writeInt(splitVersion);
    }

    /**
     * If there are already serialized splits in <code>splitsProto</code>, then write those splits, else
     * serialize and writes the splits.
     * @param conf
     * @param array
     * @param out
     * @param splitsProto
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    private static <T extends InputSplit> SplitMetaInfo[] writeSplits(Configuration conf, T[] array,
            FSDataOutputStream out, MRSplitsProto splitsProto) throws IOException, InterruptedException {
        SplitMetaInfo[] info = null;
        if (array.length != 0) {
            info = new SplitMetaInfo[array.length];
            SerializationFactory factory = new SerializationFactory(conf);
            int maxBlockLocations = conf.getInt(MRConfig.MAX_BLOCK_LOCATIONS_KEY, MRConfig.MAX_BLOCK_LOCATIONS_DEFAULT);
            long offset = out.getPos();
            int i = 0;
            for(MRSplitProto splitProto : splitsProto.getSplitsList()) {
                long prevCount = out.getPos();
                Text.writeString(out, splitProto.getSplitClassName());
                splitProto.getSplitBytes().writeTo(out);
                info[i++] = createSplitMetaInfo(array[i], offset, maxBlockLocations);
                offset += out.getPos() - prevCount;
            }
            while(i < array.length) {
                long prevCount = out.getPos();
                Text.writeString(out, array[i].getClass().getName());
                Serializer<T> serializer = factory.getSerializer((Class<T>) array[i].getClass());
                serializer.open(out);
                serializer.serialize(array[i]);
                info[i++] = createSplitMetaInfo(array[i], offset, maxBlockLocations);
                offset += out.getPos() - prevCount;
            }
        }
        LOG.info("Size of serialized job.split file is " + out.getPos());
        return info;
    }

    /**
     * Serializes split and write to given FSDataOutputStream.
     * If splitProto contains already serialized splits, write those to given FSDataOutputStream.
     * @param split
     * @param offset
     * @param maxBlockLocations
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private static <T extends InputSplit> SplitMetaInfo createSplitMetaInfo(T split,
            long offset, int maxBlockLocations) throws IOException, InterruptedException {
        String[] locations = split.getLocations();
        if (locations.length > maxBlockLocations) {
            LOG.warn("Max block location exceeded for split: " + split + " splitsize: " + locations.length
                    + " maxsize: " + maxBlockLocations);
            locations = Arrays.copyOf(locations, maxBlockLocations);
        }
        return new JobSplit.SplitMetaInfo(locations, offset, split.getLength());
    }

    private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, FsPermission p, int splitMetaInfoVersion,
            JobSplit.SplitMetaInfo[] allSplitMetaInfo) throws IOException {
        // write the splits meta-info to a file for the job tracker
        FSDataOutputStream out = FileSystem.create(fs, filename, p);
        out.write(META_SPLIT_FILE_HEADER);
        WritableUtils.writeVInt(out, splitMetaInfoVersion);
        WritableUtils.writeVInt(out, allSplitMetaInfo.length);
        for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
            splitMetaInfo.write(out);
        }
        out.close();
    }

}
