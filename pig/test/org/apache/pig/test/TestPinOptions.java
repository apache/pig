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

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.pig.PigServer;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.junit.Before;
import org.junit.Test;

public class TestPinOptions {

    protected PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(LOCAL);
    }

    @Test
    public void testPinnedJoinOption() throws Exception {
        String[] joinTypes = {"hash", "repl", "merge", "skewed", "default"};
        String[] expectedJoinTypes = {"HASH", "REPLICATED", "MERGE", "SKEWED", "HASH"};
        for (int i = 0; i < joinTypes.length; i++) {
            pigServer.setBatchOn();
            pigServer.registerQuery("a = load 'tmp1' as (foo, bar);");
            pigServer.registerQuery("b = load 'tmp1' as (foo, bar);");
            pigServer.registerQuery("c = join a by foo, b by foo using '"+joinTypes[i]+"';");
            pigServer.registerQuery("store c into 'tmp2';");
            LOJoin op = (LOJoin)TestPigStats.getLogicalPlan(pigServer).findByAlias("c");
            assertTrue("did "+joinTypes[i]+" join get pinned? ",
                    op.isPinnedOption(LOJoin.OPTION_JOIN));
            assertEquals("did the right join type get set? ",
                    ((LOJoin) op).getJoinType().toString(), expectedJoinTypes[i]);
            pigServer.discardBatch();
        }
    }

    @Test
    public void testNotPinnedJinOption() throws Exception {
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load 'tmp1' as (foo, bar);");
        pigServer.registerQuery("b = load 'tmp1' as (foo, bar);");
        pigServer.registerQuery("c = join a by foo, b by foo;");
        pigServer.registerQuery("store c into 'tmp2';");
        LOJoin op = (LOJoin)TestPigStats.getLogicalPlan(pigServer).findByAlias("c");
        assertEquals("default join should be hash",
                ((LOJoin) op).getJoinType().toString(), "HASH");
        assertFalse(op.isPinnedOption(LOJoin.OPTION_JOIN));
        pigServer.discardBatch();
    }

    @Test
    public void testGroupOptions() throws Exception {
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load 'tmp1' as (foo, bar);");
        pigServer.registerQuery("b = group a by foo;");
        pigServer.registerQuery("store b into 'tmp2';");

        LOCogroup op = (LOCogroup)TestPigStats.getLogicalPlan(pigServer).findByAlias("b");
        assertFalse(op.isPinnedOption(LOCogroup.OPTION_GROUPTYPE));
        pigServer.discardBatch();

        pigServer.setBatchOn();
        pigServer.registerQuery("a = load 'tmp' as (foo, bar);");
        pigServer.registerQuery("b = group a by foo using 'collected';");
        pigServer.registerQuery("store b into 'tmp2';");
        op = (LOCogroup)TestPigStats.getLogicalPlan(pigServer).findByAlias("b");
        assertTrue(op.isPinnedOption(LOCogroup.OPTION_GROUPTYPE));
        pigServer.discardBatch();
    }
}