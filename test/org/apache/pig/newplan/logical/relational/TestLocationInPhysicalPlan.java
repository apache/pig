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

package org.apache.pig.newplan.logical.relational;

import java.io.File;
import java.util.List;

import org.junit.Assert;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.OriginalLocation;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.JobStats;
import org.junit.Test;

public class TestLocationInPhysicalPlan {

    @Test
    public void test() throws Exception {
        File input = File.createTempFile("test", "input");
        input.deleteOnExit();
        File output = File.createTempFile("test", "output");
        output.delete();
        Util.createLocalInputFile(input.getAbsolutePath(), new String[] {
            "1,2,3",
            "1,1,3",
            "1,1,1",
            "3,1,1",
            "1,2,1",
        });
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        pigServer.setBatchOn();
        pigServer.registerQuery(
                "A = LOAD '" + Util.encodeEscape(input.getAbsolutePath()) + "' using PigStorage();\n"
            +  	"B = GROUP A BY $0;\n"
            + 	"A = FOREACH B GENERATE COUNT(A);\n"
            +	"STORE A INTO '" + Util.encodeEscape(output.getAbsolutePath()) + "';");
        ExecJob job = pigServer.executeBatch().get(0);
        List<OriginalLocation> originalLocations = job.getPOStore().getOriginalLocations();
        Assert.assertEquals(1, originalLocations.size());
        OriginalLocation originalLocation = originalLocations.get(0);
        Assert.assertEquals(4, originalLocation.getLine());
        Assert.assertEquals(0, originalLocation.getOffset());
        Assert.assertEquals("A", originalLocation.getAlias());
        JobStats jStats = (JobStats)job.getStatistics().getJobGraph().getSinks().get(0);
        if (Util.getLocalTestMode().toString().equals("TEZ_LOCAL")) {
            Assert.assertEquals("A[1,4],A[3,4],B[2,4]", jStats.getAliasLocation());
        } else {
            Assert.assertEquals("M: A[1,4],A[3,4],B[2,4] C: A[3,4],B[2,4] R: A[3,4]", jStats.getAliasLocation());
        }
    }
}
