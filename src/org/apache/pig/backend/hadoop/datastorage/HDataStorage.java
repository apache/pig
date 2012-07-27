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

import java.net.URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;

public class HDataStorage implements DataStorage {

    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";

    private FileSystem fs;
    private Configuration configuration;
    private Properties properties;
    private URI uri;

    public HDataStorage(URI uri, Properties properties) {
        this.properties = properties;
        this.uri = uri;
        init();
    }

    public HDataStorage(Properties properties) {
        this.properties = properties;
        init();
    }

    public void init() {
        // check if name node is set, if not we set local as fail back
        String nameNode = this.properties.getProperty(FILE_SYSTEM_LOCATION);
        if (nameNode == null || nameNode.length() == 0) {
            nameNode = "local";
        }
        this.configuration = ConfigurationUtil.toConfiguration(this.properties);
        try {
            if (this.uri != null) {
                this.fs = FileSystem.get(this.uri, this.configuration);
            } else {
                this.fs = FileSystem.get(this.configuration);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create DataStorage", e);
        }
    }

    public void close() throws IOException {
        fs.close();
    }
    
    public Properties getConfiguration() {
        return this.properties;
    }

    public void updateConfiguration(Properties newConfiguration)
            throws DataStorageException {
        // TODO sgroschupf 25Feb2008 this method is never called and
        // I'm even not sure if hadoop would support that, I doubt it.

        if (newConfiguration == null) {
            return;
        }
        
        Enumeration<Object> newKeys = newConfiguration.keys();
        
        while (newKeys.hasMoreElements()) {
            String key = (String) newKeys.nextElement();
            String value = null;
            
            value = newConfiguration.getProperty(key);
            
            fs.getConf().set(key,value);
        }
    }
    
    public Map<String, Object> getStatistics() throws IOException {
        Map<String, Object> stats = new HashMap<String, Object>();

        long usedBytes = fs.getUsed();
        stats.put(USED_BYTES_KEY , Long.valueOf(usedBytes).toString());
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            long rawCapacityBytes = dfs.getRawCapacity();
            stats.put(RAW_CAPACITY_KEY, Long.valueOf(rawCapacityBytes).toString());
            
            long rawUsedBytes = dfs.getRawUsed();
            stats.put(RAW_USED_KEY, Long.valueOf(rawUsedBytes).toString());
        }
        
        return stats;
    }
    
    public ElementDescriptor asElement(String name) throws DataStorageException {
        if (this.isContainer(name)) {
            return new HDirectory(this, name);
        }
        else {
            return new HFile(this, name);
        }
    }
    
    public ElementDescriptor asElement(ElementDescriptor element)
            throws DataStorageException {
        return asElement(element.toString());
    }
    
    public ElementDescriptor asElement(String parent,
                                                  String child) 
            throws DataStorageException {
        return asElement((new Path(parent, child)).toString());
    }

    public ElementDescriptor asElement(ContainerDescriptor parent,
                                                  String child) 
            throws DataStorageException {
        return asElement(parent.toString(), child);
    }

    public ElementDescriptor asElement(ContainerDescriptor parent,
                                                  ElementDescriptor child) 
            throws DataStorageException {
        return asElement(parent.toString(), child.toString());
    }

    public ContainerDescriptor asContainer(String name) 
            throws DataStorageException {
        return new HDirectory(this, name);
    }
    
    public ContainerDescriptor asContainer(ContainerDescriptor container)
            throws DataStorageException {
        return new HDirectory(this, container.toString());
    }
    
    public ContainerDescriptor asContainer(String parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent, child);
    }

    public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child);
    }
    
    public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                      ContainerDescriptor child)
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child.toString());
    }
    
    public void setActiveContainer(ContainerDescriptor container) {
        fs.setWorkingDirectory(new Path(container.toString()));
    }
    
    public ContainerDescriptor getActiveContainer() {
        return new HDirectory(this, fs.getWorkingDirectory());
    }

    public boolean isContainer(String name) throws DataStorageException {
        boolean isContainer = false;
        Path path = new Path(name);
        
        try {
            if ((this.fs.exists(path)) && (! this.fs.isFile(path))) {
                isContainer = true;
            }
        }
        catch (IOException e) {
            int errCode = 6007;
            String msg = "Unable to check name " + name;
            throw new DataStorageException(msg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
        
        return isContainer;
    }
    
    public HPath[] asCollection(String pattern) throws DataStorageException {
        try {
            FileStatus[] paths = this.fs.globStatus(new Path(pattern));

            if (paths == null)
                return new HPath[0];

            List<HPath> hpaths = new ArrayList<HPath>();
            
            for (int i = 0; i < paths.length; ++i) {
                HPath hpath = (HPath)this.asElement(paths[i].getPath().toString());
                if (!hpath.systemElement()) {
                    hpaths.add(hpath);
                }
            }

            return hpaths.toArray(new HPath[hpaths.size()]);
        } catch (IOException e) {
            int errCode = 6008;
            String msg = "Failed to obtain glob for " + pattern;
            throw new DataStorageException(msg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
    }
    
    public FileSystem getHFS() {
        return fs;
    }
}
