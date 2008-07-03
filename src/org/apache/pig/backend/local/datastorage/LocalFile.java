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


package org.apache.pig.backend.local.datastorage;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.pig.backend.datastorage.*;
import org.apache.pig.impl.util.WrappedIOException;

public class LocalFile extends LocalPath {

    public LocalFile(LocalDataStorage fs, String path) {
        super(fs, path);
    }
    
    public LocalFile(LocalDataStorage fs, File path) {
        super(fs, path);
    }

    public LocalFile(LocalDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public LocalFile(LocalDataStorage fs, File parent, File child) {
        super(fs,
              parent.getPath(),
              child.getPath());
    }
    
    public LocalFile(LocalDataStorage fs, File parent, String child) {
        this(fs, parent.getPath(), child);
    }
    
    public LocalFile(LocalDataStorage fs, String parent, File child) {
        this(fs, parent, child.getPath());
    }
        
    @Override
    public OutputStream create(Properties configuration) 
            throws IOException {
        if (! getCurPath().createNewFile()) {
            throw new IOException("Failed to create file " + this.path);
        }
        
        return new FileOutputStream(getCurPath());
    }    
    
    @Override
    public void copy(ElementDescriptor dstName,
                     Properties dstConfiguration,
            boolean removeSrc) 
            throws IOException {
        if (dstName == null) {
            return;
        }
        
        if (!exists()) {
            throw new IOException("Source does not exist " +
                                  this);
        }

        if (dstName.exists()) {
            if (dstName instanceof ContainerDescriptor) {
                try {
                    dstName = dstName.getDataStorage().
                                      asElement((ContainerDescriptor) dstName,
                                                path.getName());
                }
                catch (DataStorageException e) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Unable to generate element name (src: ");
                    sb.append(this);
                    sb.append(", dst: ");
                    sb.append(dstName);
                    sb.append(")");
                    throw WrappedIOException.wrap(sb.toString(), e);
                }
            }
        }
        
        InputStream in = null;
        OutputStream out = null;
        
        in = this.open();
        out = dstName.create(dstConfiguration);
            
        byte[] data = new byte[4 * 1024];
        int bc;
        while((bc = in.read(data)) != -1) {
            out.write(data, 0, bc);
        }
        
        out.close();
            
        if (removeSrc) {
            delete();
        }
    }    

    public InputStream open (Properties configuration) throws IOException {
        return open();
    }
    
    public InputStream open () throws IOException {
        return new FileInputStream(this.path);
    }
    
    public SeekableInputStream sopen(Properties configuration) throws IOException {
        return sopen();
    }
    
    public SeekableInputStream sopen() throws IOException {
        try {
            return new LocalSeekableInputStream(this.path);
        }
        catch (FileNotFoundException e) {
            throw WrappedIOException.wrap("Unable to find " + this.path, e);
        }
    }
}
