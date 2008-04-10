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

import java.io.*;
import java.util.Properties;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;

/**
 * This class builds a single instance of itself with the Singleton 
 * design pattern. While building the single instance, it sets up a 
 * mini cluster that actually consists of a mini DFS cluster and a 
 * mini MapReduce cluster on the local machine and also sets up the 
 * environment for Pig to run on top of the mini cluster.
 */
public class MiniCluster {
	private MiniDFSCluster m_dfs = null;
	private MiniMRCluster m_mr = null;
	private FileSystem m_fileSys = null;
	private JobConf m_conf = null;
	
	private final static MiniCluster INSTANCE = new MiniCluster();
	
	private MiniCluster() {
		setupMiniDfsAndMrClusters();
	}
	
	private void setupMiniDfsAndMrClusters() {
		try {
			final int dataNodes = 4;     // There will be 4 data nodes
			final int taskTrackers = 4;  // There will be 4 task tracker nodes
			Configuration config = new Configuration();
			
            // Builds and starts the mini dfs and mapreduce clusters
            m_dfs = new MiniDFSCluster(config, dataNodes, true, null);
            m_fileSys = m_dfs.getFileSystem();
            m_mr = new MiniMRCluster(taskTrackers, m_fileSys.getName(), 1);
            
            // Create the configuration hadoop-site.xml file
            File conf_dir = new File(System.getProperty("user.home"), "pigtest/conf/");
            conf_dir.mkdirs();
            File conf_file = new File(conf_dir, "hadoop-site.xml");
            
            // Write the necessary config info to hadoop-site.xml
            m_conf = m_mr.createJobConf();      
            m_conf.setInt("mapred.submit.replication", 2);
            m_conf.set("dfs.datanode.address", "0.0.0.0:0");
            m_conf.set("dfs.datanode.http.address", "0.0.0.0:0");
            m_conf.write(new FileOutputStream(conf_file));
            
            // Set the system properties needed by Pig
            System.setProperty("cluster", m_conf.get("mapred.job.tracker"));
            System.setProperty("namenode", m_conf.get("fs.default.name"));
            System.setProperty("junit.hadoop.conf", conf_dir.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
	}
	
    /**
     * Returns the single instance of class MiniClusterBuilder that
     * represents the resouces for a mini dfs cluster and a mini 
     * mapreduce cluster. 
     */
	public static MiniCluster buildCluster() {
		return INSTANCE;
	}
	
	protected void finalize() {
		shutdownMiniDfsAndMrClusters();
	}
	
	private void shutdownMiniDfsAndMrClusters() {
		try {
			if (m_fileSys != null) { m_fileSys.close(); }
		} catch (IOException e) {
	    	e.printStackTrace();
		}
		if (m_dfs != null) { m_dfs.shutdown(); }
		if (m_mr != null) { m_mr.shutdown(); }		
	}

    public Properties getProperties() {
        return ConfigurationUtil.toProperties(m_conf);
    }
}
