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
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkExecType;

public class SparkMiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File CONF_FILE = new File(CONF_DIR, "hadoop-site.xml");
    private ExecType spark = new SparkExecType();

    SparkMiniCluster() {

    }

    @Override
    public ExecType getExecType() {
        return spark;
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
        try {
            CONF_DIR.mkdirs();
            if (CONF_FILE.exists()) {
                CONF_FILE.delete();
            }
            m_conf = new Configuration();
            m_conf.set("io.sort.mb", "1");
            m_conf.writeXml(new FileOutputStream(CONF_FILE));
            int dataNodes = 4;
            m_dfs = new MiniDFSCluster(m_conf, dataNodes, true, null);
            m_fileSys = m_dfs.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    @Override
    protected void shutdownMiniMrClusters() {
        if (CONF_FILE.exists()) {
            CONF_FILE.delete();
        }
    }

}
