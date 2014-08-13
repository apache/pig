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
package org.apache.pig.impl.io;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.io.file.tfile.TFile.Reader;

/**
 * A record reader used to read data written using {@link InterRecordWriter} It
 * uses the default InterSedes object for deserialization.
 */
public class TFileRecordReader extends RecordReader<Text, Tuple> {

    private long start;
    private long end;
    Reader reader = null;
    Reader.Scanner scanner = null;
    private Tuple value = null;
    private FSDataInputStream fileIn = null;
    private static InterSedes sedes = InterSedesFactory.getInterSedesInstance();

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                    throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(split.getPath());
        reader = new Reader(fileIn, fs.getFileStatus(file).getLen(), job);
        scanner = reader.createScannerByByteRange(start, split.getLength());
    }

    public boolean nextKeyValue() throws IOException {
        //    skip to next record
        if (scanner.atEnd()) {
            value = null;
            return false;
        }

        DataInputStream in = scanner.entry().getValueStream();
        try {
            // if we got here, we have seen RECORD_1-RECORD_2-RECORD_3-TUPLE_MARKER
            // sequence - lets now read the contents of the tuple 
            value = (Tuple) sedes.readDatum(in);
        }
        finally {
            in.close();
        }
        
        scanner.advance();
        return true;
    }

    @Override
    public Text getCurrentKey() {
        // the key is always null since we don't really have a key for each
        // input record
        return null;
    }

    @Override
    public Tuple getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        }
        else {
            //TFile.Reader reads into buffer so progress is updated in chunks.
            return Math.min(1.0f,
                      (fileIn.getPos() - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (scanner != null) {
            scanner.close();
        }
        if (reader != null) reader.close();
        if (fileIn != null) fileIn.close();
    }
}
