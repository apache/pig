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

package org.apache.pig.backend.hadoop.datastorage;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;

public class ConfigurationUtil {

    public static Configuration toConfiguration(Properties properties) {
        assert properties != null;
        final Configuration config = new Configuration(false);
        final Enumeration<Object> iter = properties.keys();
        while (iter.hasMoreElements()) {
            final String key = (String) iter.nextElement();
            final String val = properties.getProperty(key);
            config.set(key, val);
        }
        return config;
    }

    public static Properties toProperties(Configuration configuration) {
        Properties properties = new Properties();
        assert configuration != null;
        Iterator<Map.Entry<String, String>> iter = configuration.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    /**
     * @param origConf
     * @param replaceConf
     */
    public static void mergeConf(Configuration origConf,
            Configuration replaceConf) {
        for (Entry<String, String> entry : replaceConf) {
            origConf.set(entry.getKey(), entry.getValue());
        }

    }

    public static Properties getLocalFSProperties() {
        Configuration localConf;
        if (PigMapReduce.sJobContext!=null && PigMapReduce.sJobContext.getConfiguration().get("exectype").equals(ExecType.LOCAL.toString())) {
            localConf = new Configuration(false);
            localConf.addResource("core-default.xml");
        } else {
            localConf = new Configuration(true);
            // It's really hacky, try to get unit test working under hadoop 23.
            // Hadoop23 MiniMRCluster currently need setup Distributed cache before start,
            // so build/classes/hadoop-site.xml contains such entry. This prevents some tests from
            // successful (They expect those files in hdfs), so we need to unset it in hadoop 23.
            // This should go away once MiniMRCluster fix the distributed cache issue.
            HadoopShims.unsetConf(localConf, "mapreduce.job.cache.files");
        }
        localConf.set(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
        Properties props = ConfigurationUtil.toProperties(localConf);
        return props;
    }

    public static void replaceConfigForLocalMode(Configuration configuration) {
        for (Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.startsWith(PigConstants.PIG_LOCAL_CONF_PREFIX)) {
                String realConfKey = key.substring(PigConstants.PIG_LOCAL_CONF_PREFIX.length());
                configuration.set(realConfKey, value);
            }
        }
    }
}
