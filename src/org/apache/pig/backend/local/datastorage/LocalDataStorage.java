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

import java.io.IOException;
import java.io.File;
import java.util.Properties;
import java.util.Map;

import org.apache.pig.backend.datastorage.*;

public class LocalDataStorage implements DataStorage {

    protected File workingDir;
    
    public LocalDataStorage() {
        workingDir = new File(System.getProperty("user.dir"));
    }
    
    public void init() {
        ;
    }

    public void close() throws IOException {
        ;
    }
    
    public Properties getConfiguration() {
        Properties config = new Properties();
        
        config.put(DEFAULT_REPLICATION_FACTOR_KEY, "" + 1);
        
        return config;
    }
    
    public void updateConfiguration(Properties newConfiguration) 
         throws DataStorageException {
        ;
    }
    
    public Map<String, Object> getStatistics() throws IOException {
        throw new UnsupportedOperationException();
    }
            
    public LocalPath asElement(String name) 
            throws DataStorageException {
        if (this.isContainer(name)) {
            return new LocalDir(this, name);
        }
        else {            
            return new LocalFile(this, name);
        }
    }

    public LocalPath asElement(ElementDescriptor element)
            throws DataStorageException {
        return asElement(element.toString());
    }
    
    public LocalPath asElement(String parent,
                               String child) 
            throws DataStorageException {
        return asElement((new File(parent, child).toString()));
    }

    public LocalPath asElement(ContainerDescriptor parent,
                               String child) 
            throws DataStorageException {
        return asElement(parent.toString(), child);
    }

    public LocalPath asElement(ContainerDescriptor parent,
                               ElementDescriptor child)
            throws DataStorageException {
        return asElement(parent.toString(), child.toString());
    }
    
    public LocalDir asContainer(String name) 
            throws DataStorageException {
        return new LocalDir(this, name);
    }
    
    public LocalDir asContainer(ContainerDescriptor container)
            throws DataStorageException {
        return new LocalDir(this, container.toString());
    }
    
    public LocalDir asContainer(String parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent, child);
    }

    public LocalDir asContainer(ContainerDescriptor parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent.toString(), child);
    }
    
    public LocalDir asContainer(ContainerDescriptor parent,
                                ContainerDescriptor child) 
        throws DataStorageException {
        return new LocalDir(this, parent.toString(), child.toString());
    }

    public boolean isContainer(String name) throws DataStorageException {
        boolean isContainer = false;
        File file = new File(name);
        
        if (file.exists() && file.isDirectory()) {
            isContainer = true;
        }
        
        return isContainer;
    }
    
    public void setActiveContainer(ContainerDescriptor container) {
        this.workingDir = new File(container.toString());
    }
    
    public ContainerDescriptor getActiveContainer() {
        return new LocalDir(this, this.workingDir.getPath());
    }
    
    public LocalPath[] asCollection(String pattern) throws DataStorageException {
        throw new UnsupportedOperationException();
    }
    
    public File getWorkingDir() {
        return this.workingDir;
    }
}
