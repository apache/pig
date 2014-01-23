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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.*;
import org.apache.pig.backend.executionengine.ExecException;

public abstract class HPath implements ElementDescriptor {

    protected Path path;
    protected HDataStorage fs;

    public HPath(HDataStorage fs, Path parent, Path child) {
        this.path = new Path(parent, child);
        this.fs = fs;
    }

    public HPath(HDataStorage fs, String parent, String child) {
        this(fs, new Path(parent), new Path(child));
    }

    public HPath(HDataStorage fs, Path parent, String child) {
        this(fs, parent, new Path(child));
    }

    public HPath(HDataStorage fs, String parent, Path child) {
        this(fs, new Path(parent), child);
    }

    public HPath(HDataStorage fs, String pathString) {
        this(fs, new Path(pathString));
    }

    public HPath(HDataStorage fs, Path path) {
        this.path = path;
        this.fs = fs;
    }

    public DataStorage getDataStorage() {
        return fs;
    }

    public abstract OutputStream create(Properties configuration)
             throws IOException;

    public void copy(ElementDescriptor dstName,
                     Properties dstConfiguration,
                     boolean removeSrc)
            throws IOException {
        FileSystem srcFS = this.fs.getHFS();
        FileSystem dstFS = ((HPath)dstName).fs.getHFS();

        Path srcPath = this.path;
        Path dstPath = ((HPath)dstName).path;

        boolean result = FileUtil.copy(srcFS,
                                       srcPath,
                                       dstFS,
                                       dstPath,
                                       false,
                                       new Configuration());

        if (!result) {
            int errCode = 2097;
            String msg = "Failed to copy from: " + this.toString() +
            " to: " + dstName.toString();
            throw new ExecException(msg, errCode, PigException.BUG);
        }
    }

    public abstract InputStream open() throws IOException;

    public abstract SeekableInputStream sopen() throws IOException;

    public boolean exists() throws IOException {
        return fs.getHFS().exists(path);
    }

    public void rename(ElementDescriptor newName)
             throws IOException {
        if (newName != null) {
            fs.getHFS().rename(path, ((HPath)newName).path);
        }
    }

    public void delete() throws IOException {
        // the file is removed and not placed in the trash bin
        fs.getHFS().delete(path, true);
    }

    public void setPermission(FsPermission permission) throws IOException {
        fs.getHFS().setPermission(path, permission);
    }

    public Properties getConfiguration() throws IOException {
        HConfiguration props = new HConfiguration();

        long blockSize = fs.getHFS().getFileStatus(path).getBlockSize();

        short replication = fs.getHFS().getFileStatus(path).getReplication();

        props.setProperty(BLOCK_SIZE_KEY, (Long.valueOf(blockSize)).toString());
        props.setProperty(BLOCK_REPLICATION_KEY, (Short.valueOf(replication)).toString());

        return props;
    }

    public void updateConfiguration(Properties newConfig) throws IOException {
        if (newConfig == null) {
            return;
        }

        String blkReplStr = newConfig.getProperty(BLOCK_REPLICATION_KEY);

        fs.getHFS().setReplication(path,
                                   new Short(blkReplStr).shortValue());
    }

    public Map<String, Object> getStatistics() throws IOException {
        HashMap<String, Object> props = new HashMap<String, Object>();

        FileStatus fileStatus = fs.getHFS().getFileStatus(path);

        props.put(BLOCK_SIZE_KEY, fileStatus.getBlockSize());
        props.put(BLOCK_REPLICATION_KEY, fileStatus.getReplication());
        props.put(LENGTH_KEY, fileStatus.getLen());
        props.put(MODIFICATION_TIME_KEY, fileStatus.getModificationTime());

        return props;
    }

    public OutputStream create() throws IOException {
        return create(null);
    }

    public void copy(ElementDescriptor dstName,
                     boolean removeSrc)
            throws IOException {
        copy(dstName, null, removeSrc);
    }

    public Path getPath() {
        return path;
    }

    public FileSystem getHFS() {
        return fs.getHFS();
    }

    public boolean systemElement() {
        return (path != null &&
                (path.getName().startsWith("_") ||
                 path.getName().startsWith(".")));
    }

    @Override
    public String toString() {
        return path.makeQualified(getHFS()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof HPath)) {
            return false;
        }

        return this.path.equals(((HPath)obj).path);
    }

    public int compareTo(ElementDescriptor other) {
        return path.compareTo(((HPath)other).path);
    }

    @Override
    public int hashCode() {
        return this.path.hashCode();
    }
}
