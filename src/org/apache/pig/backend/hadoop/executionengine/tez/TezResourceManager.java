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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.PigConfiguration;
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
    private static final Log log = LogFactory.getLog(TezResourceManager.class);

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
                boolean cacheEnabled =
                        conf.getBoolean(PigConfiguration.PIG_USER_CACHE_ENABLED, false);

                if(cacheEnabled){
                    Path pathOnDfs = getFromCache(pigContext, conf, uri.toURL());
                    if(pathOnDfs != null) {
                        resources.put(resourceName, pathOnDfs);
                        return pathOnDfs;
                    }

                }

                    Path remoteFsPath = remoteFs.makeQualified(new Path(stagingDir, resourceName));
                    remoteFs.copyFromLocalFile(resourcePath, remoteFsPath);
                    remoteFs.setReplication(remoteFsPath, (short) conf.getInt(Job.SUBMIT_REPLICATION, 3));
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
    private static Path getFromCache(PigContext pigContext,
                                     Configuration conf,
                                     URL url) throws IOException {
        InputStream is1 = null;
        InputStream is2 = null;
        OutputStream os = null;

        try {
            Path stagingDir = getCacheStagingDir(conf);
            String filename = FilenameUtils.getName(url.getPath());

            is1 = url.openStream();
            String checksum = DigestUtils.shaHex(is1);
            FileSystem fs = FileSystem.get(conf);
            Path cacheDir = new Path(stagingDir, checksum);
            Path cacheFile = new Path(cacheDir, filename);
            if (fs.exists(cacheFile)) {
                log.debug("Found " + url + " in jar cache at "+ cacheDir);
                long curTime = System.currentTimeMillis();
                fs.setTimes(cacheFile, -1, curTime);
                return cacheFile;
            }
            log.info("Url "+ url + " was not found in jarcache at "+ cacheDir);
            // attempt to copy to cache else return null
            fs.mkdirs(cacheDir, FileLocalizer.OWNER_ONLY_PERMS);
            is2 = url.openStream();
            os = FileSystem.create(fs, cacheFile, FileLocalizer.OWNER_ONLY_PERMS);
            IOUtils.copyBytes(is2, os, 4096, true);

            return cacheFile;

        } catch (IOException ioe) {
            log.info("Unable to retrieve jar from jar cache ", ioe);
            return null;
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(is1);
            org.apache.commons.io.IOUtils.closeQuietly(is2);
            // IOUtils should not close stream to HDFS quietly
            if (os != null) {
                os.close();
            }
        }
    }


    private static Path getCacheStagingDir(Configuration conf) throws IOException {
        String pigTempDir = conf.get(PigConfiguration.PIG_USER_CACHE_LOCATION,
                conf.get(PigConfiguration.PIG_TEMP_DIR, "/tmp"));
        String currentUser = System.getProperty("user.name");
        Path stagingDir = new Path(pigTempDir + "/" + currentUser + "/", ".pigcache");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(stagingDir);
        fs.setPermission(stagingDir, FileLocalizer.OWNER_ONLY_PERMS);
        return stagingDir;
    }
}

