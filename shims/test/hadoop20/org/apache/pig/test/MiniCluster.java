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
package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;

public class MiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File CONF_FILE = new File(CONF_DIR, "hadoop-site.xml");

    private MiniMRCluster m_mr = null;

    /**
     * @deprecated use {@link org.apache.pig.test.MiniGenericCluster.buildCluster() instead.
     */
    @Deprecated
    public static MiniCluster buildCluster() {
        System.setProperty("test.exec.type", "mr");
        return (MiniCluster)MiniGenericCluster.buildCluster();
    }

    @Override
    protected ExecType getExecType() {
        return ExecType.MAPREDUCE;
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
        try {
            System.setProperty("hadoop.log.dir", "build/test/logs");
            final int dataNodes = 4;     // There will be 4 data nodes
            final int taskTrackers = 4;  // There will be 4 task tracker nodes

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
            m_mr = new MiniMRCluster(taskTrackers, m_fileSys.getUri().toString(), 1);

            // Write the necessary config info to hadoop-site.xml
            m_conf = m_mr.createJobConf();
            m_conf.setInt(MRConfiguration.SUMIT_REPLICATION, 2);
            m_conf.setInt(MRConfiguration.MAP_MAX_ATTEMPTS, 2);
            m_conf.setInt(MRConfiguration.REDUCE_MAX_ATTEMPTS, 2);
            m_conf.set("dfs.datanode.address", "0.0.0.0:0");
            m_conf.set("dfs.datanode.http.address", "0.0.0.0:0");
            m_conf.set("pig.jobcontrol.sleep", "100");
            m_conf.writeXml(new FileOutputStream(CONF_FILE));

            // Set the system properties needed by Pig
            System.setProperty("cluster", m_conf.get(MRConfiguration.JOB_TRACKER));
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
        if (m_mr != null) { m_mr.shutdown(); }
            m_mr = null;
    }
}
