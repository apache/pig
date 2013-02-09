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

package org.apache.pig.backend.hadoop.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.SeekableInputStream;

public class HFile extends HPath {
    
    public HFile(HDataStorage fs, Path parent, Path child) {
        super(fs, parent, child);
    }

    public HFile(HDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public HFile(HDataStorage fs, Path parent, String child) {
        super(fs, parent, child);
    }

    public HFile(HDataStorage fs, String parent, Path child) {
        super(fs, parent, child);
    }
        
    public HFile(HDataStorage fs, String pathString) {
        super(fs, pathString);
    }
        
    public HFile(HDataStorage fs, Path path) {
        super(fs, path);
    }
    
    public OutputStream create(Properties configuration) 
             throws IOException {
        return fs.getHFS().create(path, false);
    }
    
    public InputStream open(Properties configuration) throws IOException {
        return open();
    }
    
    public InputStream open() throws IOException {
        return fs.getHFS().open(path);
    }

    public SeekableInputStream sopen(Properties configuration) throws IOException {
        return sopen();
    }
    
    public SeekableInputStream sopen() throws IOException {
        return new HSeekableInputStream(fs.getHFS().open(path),
                                        fs.getHFS().getContentSummary(path).getLength());
    }
}
