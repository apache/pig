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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;

public class TezResourceManager {
    private static TezResourceManager instance = null;
    private boolean inited = false;
    private Path stagingDir;
    private FileSystem remoteFs;
    private Configuration conf;
    private PigContext pigContext;
    public Map<String, Path> resources = new HashMap<String, Path>();

    static public TezResourceManager getInstance() {
        if (instance==null) {
            instance = new TezResourceManager();
        }
        return instance;
    }

    public void init(PigContext pigContext, Configuration conf) throws IOException {
        if (!inited) {
            this.stagingDir = FileLocalizer.getTemporaryResourcePath(pigContext);
            this.remoteFs = FileSystem.get(conf);
            this.conf = conf;
            this.pigContext = pigContext;
            this.inited = true;
        }
    }

    public Path getStagingDir() {
        return stagingDir;
    }

    // Add files from the source FS as local resources. The resource name will
    // be the same as the file name.
    public Path addTezResource(URI uri) throws IOException {
        synchronized(this) {
            Path resourcePath = new Path(uri.getPath());
            String resourceName = resourcePath.getName();

            if (resources.containsKey(resourceName)) {
                return resources.get(resourceName);
            }

            // Ship the local resource to the staging directory on the remote FS
            if (!pigContext.getExecType().isLocal() && uri.toString().startsWith("file:")) {
                Path remoteFsPath = remoteFs.makeQualified(new Path(stagingDir, resourceName));
                remoteFs.copyFromLocalFile(resourcePath, remoteFsPath);
                remoteFs.setReplication(remoteFsPath, (short)conf.getInt(Job.SUBMIT_REPLICATION, 3));
                resources.put(resourceName, remoteFsPath);
                return remoteFsPath;
            }
            resources.put(resourceName, resourcePath);
            return resourcePath;
        }
    }

    // Add files already present in the remote FS as local resources. Allow the
    // resource name to be different from the file name to to support resource
    // aliasing in a CACHE statement (and to allow the same file to be aliased
    // with multiple resource names).
    public void addTezResource(String resourceName, Path remoteFsPath) throws IOException {
        if (!resources.containsKey(resourceName)) {
            resources.put(resourceName, remoteFsPath);
        }
    }

    public Map<String, LocalResource> addTezResources(Set<URI> resources) throws Exception {
        Set<String> resourceNames = new HashSet<String>();
        for (URI uri : resources) {
            addTezResource(uri);
            resourceNames.add(new Path(uri.getPath()).getName());
        }
        return getTezResources(resourceNames);
    }

    public Map<String, LocalResource> getTezResources(Set<String> resourceNames) throws Exception {
        Map<String, LocalResource> tezResources = new HashMap<String, LocalResource>();
        for (String resourceName : resourceNames) {
            // The resource name will be symlinked to the resource path in the
            // container's working directory.
            Path resourcePath = resources.get(resourceName);
            FileStatus fstat = remoteFs.getFileStatus(resourcePath);

            LocalResource tezResource = LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromPath(fstat.getPath()),
                    LocalResourceType.FILE,
                    LocalResourceVisibility.APPLICATION,
                    fstat.getLen(),
                    fstat.getModificationTime());

            tezResources.put(resourceName, tezResource);
        }
        return tezResources;
    }
}

