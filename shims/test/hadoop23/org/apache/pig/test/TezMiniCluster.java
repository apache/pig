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
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezSessionManager;
import org.apache.tez.common.TezJobConfig;

public class TezMiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File TEZ_LIB_DIR = new File("build/ivy/lib/Pig");
    private static final File TEZ_CONF_FILE = new File(CONF_DIR, "tez-site.xml");
    private static final File CORE_CONF_FILE = new File(CONF_DIR, "core-site.xml");
    private static final File HDFS_CONF_FILE = new File(CONF_DIR, "hdfs-site.xml");
    private static final File MAPRED_CONF_FILE = new File(CONF_DIR, "mapred-site.xml");
    private static final ExecType TEZ = new TezExecType();

    protected MiniMRYarnCluster m_mr = null;
    private Configuration m_dfs_conf = null;
    private Configuration m_mr_conf = null;

    @Override
    protected ExecType getExecType() {
        return TEZ;
    }

    @Override
    public void setupMiniDfsAndMrClusters() {
        try {
            deleteConfFiles();
            CONF_DIR.mkdirs();

            // Build mini DFS cluster
            Configuration hdfsConf = new Configuration(false);
            hdfsConf.addResource("core-default.xml");
            hdfsConf.addResource("hdfs-default.xml");
            m_dfs = new MiniDFSCluster.Builder(hdfsConf)
                    .numDataNodes(2)
                    .format(true)
                    .racks(null)
                    .build();
            m_fileSys = m_dfs.getFileSystem();
            m_dfs_conf = m_dfs.getConfiguration(0);
            //Create user home directory
            m_fileSys.mkdirs(m_fileSys.getWorkingDirectory());

            m_dfs_conf.writeXml(new FileOutputStream(HDFS_CONF_FILE));
            m_fileSys.copyFromLocalFile(
                    new Path(HDFS_CONF_FILE.getAbsoluteFile().toString()),
                    new Path("/pigtest/conf/hdfs-site.xml"));
            Job job = Job.getInstance(m_dfs_conf);
            job.addFileToClassPath(new Path("/pigtest/conf/hdfs-site.xml"));

            // Build mini YARN cluster
            m_mr = new MiniMRYarnCluster("PigMiniCluster", 2);
            m_mr.init(m_dfs_conf);
            m_mr.start();
            m_mr_conf = m_mr.getConfig();
            m_mr_conf.set("mapreduce.framework.name", "yarn-tez");
            m_mr_conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    System.getProperty("java.class.path"));

            m_mr_conf.writeXml(new FileOutputStream(MAPRED_CONF_FILE));
            m_fileSys.copyFromLocalFile(
                    new Path(MAPRED_CONF_FILE.getAbsoluteFile().toString()),
                    new Path("/pigtest/conf/mapred-site.xml"));
            job.addFileToClassPath(new Path("/pigtest/conf/mapred-site.xml"));

            // Write core-site.xml
            m_conf = m_mr_conf;
            m_conf.writeXml(new FileOutputStream(CORE_CONF_FILE));
            m_fileSys.copyFromLocalFile(
                    new Path(CORE_CONF_FILE.getAbsoluteFile().toString()),
                    new Path("/pigtest/conf/core-site.xml"));
            job.addFileToClassPath(new Path("/pigtest/conf/core-site.xml"));

            // Write tez-site.xml
            Configuration tez_conf = new Configuration(false);
            // TODO PIG-3659 - Remove this once memory management is fixed
            tez_conf.set(TezJobConfig.TEZ_RUNTIME_IO_SORT_MB, "20");
            tez_conf.set("tez.lib.uris", "hdfs:///tez,hdfs:///tez/lib");
            tez_conf.writeXml(new FileOutputStream(TEZ_CONF_FILE));
            m_fileSys.copyFromLocalFile(
                    new Path(TEZ_CONF_FILE.getAbsoluteFile().toString()),
                    new Path("/pigtest/conf/tez-site.xml"));
            job.addFileToClassPath(new Path("/pigtest/conf/tez-site.xml"));

            // Copy tez jars to hdfs
            m_fileSys.mkdirs(new Path("/tez/lib"));
            FileFilter fileFilter = new RegexFileFilter("tez-.+\\.jar$");
            File[] tezJars = TEZ_LIB_DIR.listFiles(fileFilter);
            for (int i = 0; i < tezJars.length; i++) {
                if (tezJars[i].getName().startsWith("tez-api")) {
                    m_fileSys.copyFromLocalFile(
                            new Path(tezJars[i].getAbsoluteFile().toString()),
                            new Path("/tez"));
                } else {
                    m_fileSys.copyFromLocalFile(
                            new Path(tezJars[i].getAbsoluteFile().toString()),
                            new Path("/tez/lib"));
                }
            }

            // Turn FetchOptimizer off so that we can actually test Tez
            m_conf.set(PigConfiguration.OPT_FETCH, System.getProperty("test.opt.fetch", "false"));

            System.setProperty("junit.hadoop.conf", CONF_DIR.getPath());
            System.setProperty("hadoop.log.dir", "build/test/logs");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void shutdownMiniDfsAndMrClusters() {
        TezSessionManager.shutdown();
        super.shutdownMiniDfsAndMrClusters();
    }

    @Override
    protected void shutdownMiniMrClusters() {
        deleteConfFiles();
        if (m_mr != null) {
            m_mr.stop();
            m_mr = null;
        }
    }

    private void deleteConfFiles() {
        if(TEZ_CONF_FILE.exists()) {
            TEZ_CONF_FILE.delete();
        }
        if(CORE_CONF_FILE.exists()) {
            CORE_CONF_FILE.delete();
        }
        if(HDFS_CONF_FILE.exists()) {
            HDFS_CONF_FILE.delete();
        }
        if(MAPRED_CONF_FILE.exists()) {
            MAPRED_CONF_FILE.delete();
        }
    }
}
