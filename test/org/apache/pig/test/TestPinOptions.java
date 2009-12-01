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

import java.io.IOException;

import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.test.utils.*;
import org.junit.Before;
import org.junit.Test;
import static org.apache.pig.ExecType.LOCAL;

import junit.framework.TestCase;

public class TestPinOptions extends TestCase {

    protected PigServer pigServer;
    @Before
    @Override
    protected void setUp() throws Exception {
        pigServer = new PigServer(LOCAL);
    }
        
    @Test
    public void testPinnedJoinOption() throws IOException {
        String[] joinTypes = {"hash", "repl", "merge", "skewed", "default"};
        String[] expectedJoinTypes = {"HASH", "REPLICATED", "MERGE", "SKEWED", "HASH"};
        for (int i = 0; i < joinTypes.length; i++) {
            pigServer.setBatchOn();
            pigServer.registerQuery("a = load '/tmp' as (foo, bar);");
            pigServer.registerQuery("b = load '/tmp' as (foo, bar);");
            pigServer.registerQuery("c = join a by foo, b by foo using \""+joinTypes[i]+"\";");
            LogicalOperator op = getOpByAlias(pigServer.getAliases().get("c"), "c");
            assertTrue("did "+joinTypes[i]+" join get pinned? ", 
                    op.isPinnedOption(LOJoin.OPTION_JOIN));
            assertEquals("did the right join type get set? ",
                    ((LOJoin) op).getJoinType().toString(), expectedJoinTypes[i]);
            pigServer.discardBatch();
        }
    }
    
    @Test
    public void testNotPinnedJinOption() throws IOException {
        pigServer.registerQuery("a = load '/tmp' as (foo, bar);");
        pigServer.registerQuery("b = load '/tmp' as (foo, bar);");
        pigServer.registerQuery("c = join a by foo, b by foo;");
        LogicalOperator op = getOpByAlias(pigServer.getAliases().get("c"), "c");
        assertEquals("default join should be hash", 
                ((LOJoin) op).getJoinType().toString(), "HASH");
        assertFalse(op.isPinnedOption(LOJoin.OPTION_JOIN));
    }
    
    @Test
    public void testGroupOptions() throws IOException {
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load '/tmp' as (foo, bar);");
        pigServer.registerQuery("b = group a by foo;");
        
        LogicalOperator op = getOpByAlias(pigServer.getAliases().get("b"), "b");
        assertFalse(op.isPinnedOption(LOCogroup.OPTION_GROUPTYPE));
        pigServer.discardBatch();
        
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load '/tmp' as (foo, bar);");
        pigServer.registerQuery("b = group a by foo using \"collected\";");
        op = getOpByAlias(pigServer.getAliases().get("b"), "b");
        assertTrue(op.isPinnedOption(LOCogroup.OPTION_GROUPTYPE));
        pigServer.discardBatch();
    }
    
    private LogicalOperator getOpByAlias(LogicalPlan lp, String alias) {
        for (LogicalOperator op : lp) {
            if (op.getAlias().equals(alias)) return op;
        }
        return null;
    }
    
}
