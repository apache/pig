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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.Utils;

public class TezResourceManager {
    private static Path stagingDir;
    private static PigContext pigContext;
    private static Configuration conf;
    private static URL bootStrapJar;
    private static FileSystem remoteFs;

    public static Map<URL, Path> resources = new HashMap<URL, Path>();

    public static URL getBootStrapJar() {
        return bootStrapJar;
    }

    public static void initialize(Path stagingDir, PigContext pigContext, Configuration conf) throws IOException {
        TezResourceManager.stagingDir = stagingDir;
        TezResourceManager.pigContext = pigContext;
        TezResourceManager.conf = conf;
        String jar = JarManager.findContainingJar(org.apache.pig.Main.class);
        TezResourceManager.bootStrapJar = new File(jar).toURI().toURL();
        remoteFs = FileSystem.get(conf);
        addBootStrapJar();
    }

    public static void addLocalResource(URL url) throws IOException {
        if (resources.containsKey(url)) {
            return;
        }

        Path pathInHDFS = Utils.shipToHDFS(pigContext, conf, url);
        resources.put(url, pathInHDFS);
    }

    public static void addBootStrapJar() throws IOException {
        if (resources.containsKey(bootStrapJar)) {
            return;
        }

        FileSystem remoteFs = FileSystem.get(conf);

        File jobJar = File.createTempFile("Job", ".jar");
        jobJar.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(jobJar);
        JarManager.createBootStrapJar(fos, pigContext);

        // Ship the job.jar to the staging directory on hdfs
        Path remoteJarPath = remoteFs.makeQualified(new Path(stagingDir, new Path(bootStrapJar.getFile()).getName()));
        remoteFs.copyFromLocalFile(new Path(jobJar.getAbsolutePath()), remoteJarPath);

        resources.put(bootStrapJar, remoteJarPath);
    }

    public static Path get(URL url) {
        return resources.get(url);
    }

    public static Map<String, LocalResource> getTezResources(Set<URL> urls) throws IOException {
        Map<String, LocalResource> tezResources = new HashMap<String, LocalResource>();
        for (URL url : urls) {
            if (!resources.containsKey(url)) {
                addLocalResource(url);
            }
            FileStatus fstat = remoteFs.getFileStatus(resources.get(url));
            LocalResource tezResource = LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromPath(fstat.getPath()),
                    LocalResourceType.FILE,
                    LocalResourceVisibility.APPLICATION,
                    fstat.getLen(),
                    fstat.getModificationTime());
            tezResources.put(resources.get(url).getName(), tezResource);
        }
        return tezResources;
    }
}

