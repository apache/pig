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


import java.util.Properties;

import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;

import org.junit.Assert;
import org.junit.Test;

public class TestConfigurationUtil {

    @Test
    public void testExpandForAlternativeNames() {
        Properties properties = null;
        properties = ConfigurationUtil.expandForAlternativeNames("fs.df.interval", "500");
        Assert.assertEquals(1,properties.size());
        Assert.assertEquals("500",properties.get("fs.df.interval"));

        properties = ConfigurationUtil.expandForAlternativeNames("dfs.df.interval", "600");
        Assert.assertEquals(2,properties.size());
        Assert.assertEquals("600",properties.get("fs.df.interval"));
        Assert.assertEquals("600",properties.get("dfs.df.interval"));

        properties = ConfigurationUtil.expandForAlternativeNames("", "");
        Assert.assertEquals(1,properties.size());
        Assert.assertEquals("",properties.get(""));

    }
}
