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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestExecutableManager {

    @Test
    public void testSafeEnvVarName() {
        ExecutableManager manager = new ExecutableManager();
        assertEquals("foo", manager.safeEnvVarName("foo"));
        assertEquals("", manager.safeEnvVarName(""));
        assertEquals("foo_bar",manager.safeEnvVarName("foo.bar"));
        assertEquals("foo_bar",manager.safeEnvVarName("foo$bar"));
        assertEquals("foo_",manager.safeEnvVarName("foo "));
    }

    @Test
    public void testAddJobConfToEnv() {
        Configuration conf = new Configuration();
        conf.set("foo", "bar");
        Map<String, String> env = new HashMap<String, String>();
        ExecutableManager manager = new ExecutableManager();
        manager.addJobConfToEnvironment(conf, env);
        assertTrue(env.containsKey("hadoop_tmp_dir"));
        assertEquals("bar", env.get("foo"));
    }
}
