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
package org.apache.pig.bzip2r;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.tools.bzip2r.CBZip2InputStream;

@SuppressWarnings("unchecked")
public class Bzip2TextInputFormat extends PigFileInputFormat {

    /**
     * Treats keys as offset in file and value as line. Since the input file is
     * compressed, the offset for a particular line is not well-defined. This
     * implementation returns the starting position of a compressed block as the
     * key for every line in that block.
     */

    private static class BZip2LineRecordReader extends RecordReader<LongWritable,Text> {

        private long start;

        private long end;

        private long pos;

        private CBZip2InputStream in;

        private ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
        
        // flag to indicate if previous character read was Carriage Return ('\r')
        // and the next character was not Line Feed ('\n')
        private boolean CRFollowedByNonLF = false;
        
        // in the case where a Carriage Return ('\r') was not followed by a 
        // Line Feed ('\n'), this variable will hold that non Line Feed character
        // that was read from the underlying stream.
        private byte nonLFChar;
        
        
        /**
         * Provide a bridge to get the bytes from the ByteArrayOutputStream without
         * creating a new byte array.
         */
        private static class TextStuffer extends OutputStream {
            public Text target;

            @Override
            public void write(int b) {
                throw new UnsupportedOperationException("write(byte) not supported");
            }

            @Override
            public void write(byte[] data, int offset, int len) throws IOException {
                target.clear();
                target.set(data, offset, len);
            }
        }

        private TextStuffer bridge = new TextStuffer();

        private LongWritable key = new LongWritable();
        private Text value = new Text();

        public BZip2LineRecordReader(Configuration job, FileSplit split)
        throws IOException {
            start = split.getStart();
            end = start + split.getLength();
            final Path file = split.getPath();

            // open the file and seek to the start of the split
            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(split.getPath());
            fileIn.seek(start);

            in = new CBZip2InputStream(fileIn, 9, end);
            if (start != 0) {
                // skip first line and re-establish "start".
                // LineRecordReader.readLine(this.in, null);
                readLine(this.in, null);
                start = in.getPos();
            }
            pos = in.getPos();
        }

        public LongWritable createKey() {
            return new LongWritable();
        }

        public Text createValue() {
            return new Text();
        }

        /*
         * LineRecordReader.readLine() is depricated in HAdoop 0.17. So it is added here
         * locally.
         */
        private long readLine(InputStream in, 
                OutputStream out) throws IOException {
            long bytes = 0;
            while (true) {
                int b = -1;
                if(CRFollowedByNonLF) {
                    // In the previous call, a Carriage Return ('\r') was followed
                    // by a non Line Feed ('\n') character - in that call we would
                    // have not returned the non Line Feed character but would have
                    // read it from the stream - lets use that already read character
                    // now
                    b = nonLFChar;
                    CRFollowedByNonLF = false;
                } else {
                    b = in.read();
                }
                if (b == -1) {
                    break;
                }
                bytes += 1;

                byte c = (byte)b;
                if (c == '\n') {
                    break;
                }

                if (c == '\r') {
                    byte nextC = (byte)in.read();
                    if (nextC != '\n') {
                        CRFollowedByNonLF = true;
                        nonLFChar = nextC;
                    } else {
                        bytes += 1;
                    }
                    break;
                }

                if (out != null) {
                    out.write(c);
                }
            }
            return bytes;
        }

        /** Read a line. */
        public  boolean next(LongWritable key, Text value)
        throws IOException {
            if (pos > end)
                return false;

            key.set(pos); // key is position
            buffer.reset();
            // long bytesRead = LineRecordReader.readLine(in, buffer); 
            long bytesRead = readLine(in, buffer);
            if (bytesRead == 0) {
                return false;
            }
            pos = in.getPos();
            // if we have read ahead because we encountered a carriage return
            // char followed by a non line feed char, decrement the pos
            if(CRFollowedByNonLF) {
                pos--;
            }

            bridge.target = value;
            buffer.writeTo(bridge);
            return true;
        }

        /**
         * Get the progress within the split
         */
        @Override
        public float getProgress() {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        public  long getPos() throws IOException {
            return pos;
        }

        @Override
        public  void close() throws IOException {
            in.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
            // no op        
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return next(key, value);
        }

    }

    @Override
    public RecordReader createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new BZip2LineRecordReader(context.getConfiguration(), 
                (FileSplit) split);
    }

}
