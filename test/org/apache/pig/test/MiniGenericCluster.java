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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;

/**
 * This class builds a single instance of itself with the Singleton
 * design pattern. While building the single instance, it sets up a
 * mini cluster that actually consists of a mini DFS cluster and a
 * mini MapReduce cluster on the local machine and also sets up the
 * environment for Pig to run on top of the mini cluster.
 *
 * This class is the base class for MiniCluster, which has slightly
 * difference among different versions of hadoop. MiniCluster implementation
 * is located in $PIG_HOME/shims.
 */
abstract public class MiniGenericCluster {
    protected MiniDFSCluster m_dfs = null;
    protected FileSystem m_fileSys = null;
    protected Configuration m_conf = null;

    protected static MiniGenericCluster INSTANCE = null;
    protected static boolean isSetup = false;

    public static String EXECTYPE_MR = "mr";
    public static String EXECTYPE_TEZ = "tez";

    /**
     * Returns the single instance of class MiniGenericCluster that represents
     * the resources for a mini dfs cluster and a mini mr (or tez) cluster. The
     * system property "test.exec.type" is used to decide whether a mr or tez mini
     * cluster will be returned.
     */
    public static MiniGenericCluster buildCluster() {
        if (INSTANCE == null) {
            String execType = System.getProperty("test.exec.type");
            if (execType == null) {
                // Default to MR
                System.setProperty("test.exec.type", EXECTYPE_MR);
                return buildCluster(EXECTYPE_MR);
            }

            return buildCluster(execType);
        }
        return INSTANCE;
    }

    public static MiniGenericCluster buildCluster(String execType) {
        if (INSTANCE == null) {
            if (execType.equalsIgnoreCase(EXECTYPE_MR)) {
                INSTANCE = new MiniCluster();
            } else if (execType.equalsIgnoreCase(EXECTYPE_TEZ)) {
                INSTANCE = new TezMiniCluster();
            } else {
                throw new RuntimeException("Unknown test.exec.type: " + execType);
            }
        }
        if (!isSetup) {
            INSTANCE.setupMiniDfsAndMrClusters();
            isSetup = true;
        }
        return INSTANCE;
    }

    abstract public ExecType getExecType();

    abstract protected void setupMiniDfsAndMrClusters();

    public void shutDown(){
        INSTANCE.shutdownMiniDfsAndMrClusters();
    }

    @Override
    protected void finalize() {
        shutdownMiniDfsAndMrClusters();
    }

    protected void shutdownMiniDfsAndMrClusters() {
        isSetup = false;
        shutdownMiniDfsClusters();
        shutdownMiniMrClusters();
        m_conf = null;
    }

    protected void shutdownMiniDfsClusters() {
        try {
            if (m_fileSys != null) { m_fileSys.close(); }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (m_dfs != null) { m_dfs.shutdown(); }
        m_fileSys = null;
        m_dfs = null;
    }

    abstract protected void shutdownMiniMrClusters();

    public Properties getProperties() {
        errorIfNotSetup();
        return ConfigurationUtil.toProperties(m_conf);
    }

    public Configuration getConfiguration() {
        return new Configuration(m_conf);
    }

    public void setProperty(String name, String value) {
        errorIfNotSetup();
        m_conf.set(name, value);
    }

    public FileSystem getFileSystem() {
        errorIfNotSetup();
        return m_fileSys;
    }

    /**
     * Throw RunTimeException if isSetup is false
     */
    private void errorIfNotSetup(){
        if(isSetup)
            return;
        String msg = "function called on MiniCluster that has been shutdown";
        throw new RuntimeException(msg);
    }

    static public Launcher getLauncher() {
        String execType = System.getProperty("test.exec.type");
        if (execType == null) {
            System.setProperty("test.exec.type", EXECTYPE_MR);
        }
        if (execType.equalsIgnoreCase(EXECTYPE_MR)) {
            return MiniCluster.getLauncher();
        } else if (execType.equalsIgnoreCase(EXECTYPE_TEZ)) {
            return TezMiniCluster.getLauncher();
        } else {
            throw new RuntimeException("Unknown test.exec.type: " + execType);
        }
    }
}
