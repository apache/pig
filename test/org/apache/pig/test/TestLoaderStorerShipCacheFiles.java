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

import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class TestLoaderStorerShipCacheFiles {
    private static PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().inExplain = true;
    }

    @Test
    public void testShipOrcLoader() throws Exception {
        String query = "a = load 'test/org/apache/pig/builtin/orc/orc-file-11-format.orc' using OrcStorage();" +
                "store a into 'ooo';";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String hadoopVersion = "20S";
        if (Utils.isHadoop23() || Utils.isHadoop2()) {
            hadoopVersion = "23";
        }
        String[] expectedJars = new String[] {"hive-common", "hive-exec", "hive-serde", 
                "hive-shims-0." + hadoopVersion, "hive-shims-common", "kryo"};

        checkPlan(pp, expectedJars, 6, pigServer.getPigContext());
    }

    @Test
    public void testShipOrcStorer() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "store a into 'ooo' using OrcStorage;";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String hadoopVersion = "20S";
        if (Utils.isHadoop23() || Utils.isHadoop2()) {
            hadoopVersion = "23";
        }
        String[] expectedJars = new String[] {"hive-common", "hive-exec", "hive-serde", 
                "hive-shims-0." + hadoopVersion, "hive-shims-common", "kryo"};

        checkPlan(pp, expectedJars, 6, pigServer.getPigContext());
    }

    @Test
    public void testShipAvroLoader() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "store a into 'ooo' using AvroStorage();";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String[] expectedJars = new String[] {"avro-1", "avro-mapred-"};

        checkPlan(pp, expectedJars, 2, pigServer.getPigContext());
    }

    @Test
    public void testShipJsonLoader() throws Exception {
        String query = "a = load '1.txt' as (name:chararray, age:int, gpa:double);" +
                "b = order a by name;" +
                "store b into 'ooo' using JsonStorage();";
        PhysicalPlan pp = Util.buildPp(pigServer, query);

        String[] expectedJars = new String[] {"jackson-core-"};

        checkPlan(pp, expectedJars, 1, pigServer.getPigContext());
    }

    abstract protected void checkPlan(PhysicalPlan pp, String[] expectedJars, int size, PigContext pigContext) throws Exception;

    protected void assertContains(List<String> collectedFiles, String[] expectedFiles) {
        for (String expectedFile : expectedFiles) {
            boolean found = false;
            for (String collectedFile : collectedFiles) {
                if (collectedFile.contains(expectedFile)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
        }
    }
}
