/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig.piggybank.storage.avro;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.avro.file.SeekableInput;

/** Adapt an {@link FSDataInputStream} to {@link SeekableInput}. */
public class AvroStorageInputStream implements Closeable, SeekableInput {
    private final FSDataInputStream stream;
    private final long len;

    /** Construct given a path and a configuration. */
    public AvroStorageInputStream(Path path, TaskAttemptContext context) throws IOException {
        this.stream = path.getFileSystem(context.getConfiguration()).open(path);
        this.len = path.getFileSystem(context.getConfiguration()).getFileStatus(path).getLen();
    }

    public long length() {
        return len;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    public void seek(long p) throws IOException {
        stream.seek(p);
    }

    public long tell() throws IOException {
        return stream.getPos();
    }

    public void close() throws IOException {
        stream.close();
    }
}
