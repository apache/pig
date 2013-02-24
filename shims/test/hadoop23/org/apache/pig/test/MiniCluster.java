/**
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
package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;

/**
 * This class builds a single instance of itself with the Singleton
 * design pattern. While building the single instance, it sets up a
 * mini cluster that actually consists of a mini DFS cluster and a
 * mini MapReduce cluster on the local machine and also sets up the
 * environment for Pig to run on top of the mini cluster.
 */
public class MiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File CONF_FILE = new File(CONF_DIR, "hadoop-site.xml");

    protected MiniMRYarnCluster m_mr = null;
    private Configuration m_dfs_conf = null;
    private Configuration m_mr_conf = null;

    public MiniCluster() {
        super();
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
	try {
            final int dataNodes = 4;     // There will be 4 data nodes
            final int taskTrackers = 4;  // There will be 4 task tracker nodes

            System.setProperty("hadoop.log.dir", "build/test/logs");
            // Create the dir that holds hadoop-site.xml file
            // Delete if hadoop-site.xml exists already
            CONF_DIR.mkdirs();
            if(CONF_FILE.exists()) {
                CONF_FILE.delete();
            }

            // Builds and starts the mini dfs and mapreduce clusters
            Configuration config = new Configuration();
            m_dfs = new MiniDFSCluster(config, dataNodes, true, null);
            m_fileSys = m_dfs.getFileSystem();
            m_dfs_conf = m_dfs.getConfiguration(0);
            
            //Create user home directory
            m_fileSys.mkdirs(m_fileSys.getWorkingDirectory());

            m_mr = new MiniMRYarnCluster("PigMiniCluster", taskTrackers);
            m_mr.init(m_dfs_conf);
            m_mr.start();

            // Write the necessary config info to hadoop-site.xml
            m_mr_conf = new Configuration(m_mr.getConfig());

            m_conf = m_mr_conf;
			m_conf.set("fs.default.name", m_dfs_conf.get("fs.default.name"));
            m_conf.unset("mapreduce.job.cache.files");

            m_conf.setInt("mapred.io.sort.mb", 200);
            m_conf.set("mapred.child.java.opts", "-Xmx512m");

            m_conf.setInt("mapred.submit.replication", 2);
            m_conf.set("dfs.datanode.address", "0.0.0.0:0");
            m_conf.set("dfs.datanode.http.address", "0.0.0.0:0");
            m_conf.set("mapred.map.max.attempts", "2");
            m_conf.set("mapred.reduce.max.attempts", "2");
            m_conf.set("pig.jobcontrol.sleep", "100");
            m_conf.writeXml(new FileOutputStream(CONF_FILE));
            m_fileSys.copyFromLocalFile(new Path(CONF_FILE.getAbsoluteFile().toString()),
                    new Path("/pigtest/conf/hadoop-site.xml"));
            DistributedCache.addFileToClassPath(new Path("/pigtest/conf/hadoop-site.xml"), m_conf);

			System.err.println("XXX: Setting fs.default.name to: " + m_dfs_conf.get("fs.default.name"));
            // Set the system properties needed by Pig
            System.setProperty("cluster", m_conf.get("mapred.job.tracker"));
            //System.setProperty("namenode", m_dfs_conf.get("fs.default.name"));
            System.setProperty("namenode", m_conf.get("fs.default.name"));
            System.setProperty("junit.hadoop.conf", CONF_DIR.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void shutdownMiniMrClusters() {
        // Delete hadoop-site.xml on shutDown
        if(CONF_FILE.exists()) {
            CONF_FILE.delete();
        }
        if (m_mr != null) { m_mr.stop(); }
        m_mr = null;
    }
}
