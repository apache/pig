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
import java.util.Map.Entry;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.TezSessionManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class TezMiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File TEZ_LIB_DIR = new File("build/ivy/lib/Pig");
    private static final File TEZ_CONF_FILE = new File(CONF_DIR, "tez-site.xml");
    private static final File CORE_CONF_FILE = new File(CONF_DIR, "core-site.xml");
    private static final File HDFS_CONF_FILE = new File(CONF_DIR, "hdfs-site.xml");
    private static final File MAPRED_CONF_FILE = new File(CONF_DIR, "mapred-site.xml");
    private static final File YARN_CONF_FILE = new File(CONF_DIR, "yarn-site.xml");
    private static final ExecType TEZ = new TezExecType();

    protected MiniMRYarnCluster m_mr = null;
    private Configuration m_dfs_conf = null;
    private Configuration m_mr_conf = null;

    @Override
    public ExecType getExecType() {
        return TEZ;
    }

    @Override
    public void setupMiniDfsAndMrClusters() {
        try {
            deleteConfFiles();
            CONF_DIR.mkdirs();

            // Build mini DFS cluster
            Configuration hdfsConf = new Configuration();
            m_dfs = new MiniDFSCluster.Builder(hdfsConf)
                    .numDataNodes(2)
                    .format(true)
                    .racks(null)
                    .build();
            m_fileSys = m_dfs.getFileSystem();
            m_dfs_conf = m_dfs.getConfiguration(0);
            //Create user home directory
            m_fileSys.mkdirs(m_fileSys.getWorkingDirectory());

            // Write core-site.xml
            Configuration core_site = new Configuration(false);
            core_site.set(FileSystem.FS_DEFAULT_NAME_KEY, m_dfs_conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
            core_site.writeXml(new FileOutputStream(CORE_CONF_FILE));

            Configuration hdfs_site = new Configuration(false);
            for (Entry<String, String> conf : m_dfs_conf) {
                if (ArrayUtils.contains(m_dfs_conf.getPropertySources(conf.getKey()), "programatically")) {
                    hdfs_site.set(conf.getKey(), m_dfs_conf.getRaw(conf.getKey()));
                }
            }
            hdfs_site.writeXml(new FileOutputStream(HDFS_CONF_FILE));

            // Build mini YARN cluster
            m_mr = new MiniMRYarnCluster("PigMiniCluster", 2);
            m_mr.init(m_dfs_conf);
            m_mr.start();
            m_mr_conf = m_mr.getConfig();
            m_mr_conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    System.getProperty("java.class.path"));
            m_mr_conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx512m");
            m_mr_conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx512m");

            Configuration mapred_site = new Configuration(false);
            Configuration yarn_site = new Configuration(false);
            for (Entry<String, String> conf : m_mr_conf) {
                if (ArrayUtils.contains(m_mr_conf.getPropertySources(conf.getKey()), "programatically")) {
                    if (conf.getKey().contains("yarn")) {
                        yarn_site.set(conf.getKey(), m_mr_conf.getRaw(conf.getKey()));
                    } else if (!conf.getKey().startsWith("dfs")){
                        mapred_site.set(conf.getKey(), m_mr_conf.getRaw(conf.getKey()));
                    }
                }
            }

            mapred_site.writeXml(new FileOutputStream(MAPRED_CONF_FILE));
            yarn_site.writeXml(new FileOutputStream(YARN_CONF_FILE));

            // Write tez-site.xml
            Configuration tez_conf = new Configuration(false);
            // TODO PIG-3659 - Remove this once memory management is fixed
            tez_conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, "20");
            tez_conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "false");
            tez_conf.set("tez.lib.uris", "hdfs:///tez,hdfs:///tez/lib");
            // Set to a lower value so that tests don't get stuck for long because of 1 AM running at a time
            tez_conf.set(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, "20");
            // Lower the max task attempts to 2 so that negative tests fail
            // faster. By default, tasks retry 4 times
            tez_conf.set(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, "2");
            tez_conf.writeXml(new FileOutputStream(TEZ_CONF_FILE));

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

            m_conf = m_mr_conf;
            // Turn FetchOptimizer off so that we can actually test Tez
            m_conf.set(PigConfiguration.PIG_OPT_FETCH, System.getProperty("test.opt.fetch", "false"));

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
        if(YARN_CONF_FILE.exists()) {
            YARN_CONF_FILE.delete();
        }
    }

    static public Launcher getLauncher() {
        return new TezLauncher();
    }
}
