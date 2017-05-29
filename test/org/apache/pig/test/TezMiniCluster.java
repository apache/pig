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

public class TezMiniCluster extends YarnMiniCluster {

    private static final File TEZ_LIB_DIR = new File("build/ivy/lib/Pig");
    private static final File TEZ_CONF_FILE = new File(CONF_DIR, "tez-site.xml");

    private static final ExecType TEZ = new TezExecType();

    @Override
    public ExecType getExecType() {
        return TEZ;
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
        super.setupMiniDfsAndMrClusters();
        try {
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

            // Turn FetchOptimizer off so that we can actually test Tez
            m_conf.set(PigConfiguration.PIG_OPT_FETCH, System.getProperty("test.opt.fetch", "false"));

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
        super.shutdownMiniMrClusters();
    }

    @Override
    protected void deleteConfFiles() {
        super.deleteConfFiles();
        if(TEZ_CONF_FILE.exists()) {
            TEZ_CONF_FILE.delete();
        }
    }

    static public Launcher getLauncher() {
        return new TezLauncher();
    }
}
