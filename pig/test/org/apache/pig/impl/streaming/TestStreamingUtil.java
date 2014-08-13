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

package org.apache.pig.impl.streaming;

import static org.apache.pig.PigConfiguration.PIG_STREAMING_ENVIRONMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestStreamingUtil {
    private static final Random r = new Random(100L);

    @Test
    public void testAddJobConfToEnv() {
        StringBuilder streamingEnv = null;
        Configuration conf = new Configuration();
        Map<String, String> all = Maps.newHashMap();
        for (int i = 0; i < 10000; i++) {
            String key = RandomStringUtils.randomAlphanumeric(10);
            String value = RandomStringUtils.randomAlphanumeric(10);
            all.put(key, value);
        }
        Map<String, String> toInclude = Maps.newHashMap();
        for (Map.Entry<String, String> entry : all.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            conf.set(key, value);
            if (r.nextDouble() < .5) {
                toInclude.put(key, value);
                if (streamingEnv == null) {
                    streamingEnv = new StringBuilder();
                } else {
                    streamingEnv.append(",");
                }
                streamingEnv.append(key);
            }
        }
        conf.set(PIG_STREAMING_ENVIRONMENT, streamingEnv.toString());
        Map<String, String> env = new HashMap<String, String>();
        StreamingUtil.addJobConfToEnvironment(conf, env);

        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String value2 = toInclude.remove(key);
            assertEquals("Key ["+key+"] should be set in environment", value, value2);
        }
        assertTrue("There should be no remaining pairs in the included map",toInclude.isEmpty());
    }
}
