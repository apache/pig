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
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCustomSlicer extends TestCase {
    
protected final Log log = LogFactory.getLog(getClass());
    
    protected ExecType execType = ExecType.MAPREDUCE;
    
    private MiniCluster cluster;
    protected PigServer pigServer;
    
    @Before
    @Override
    protected void setUp() throws Exception {
        
        /*
        String execTypeString = System.getProperty("test.exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = PigServer.parseExecType(execTypeString);
        }
        */
        if(execType == ExecType.MAPREDUCE) {
            cluster = MiniCluster.buildCluster();
            pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        } else {
            pigServer = new PigServer(ExecType.LOCAL);
        }
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        pigServer.shutdown();
    }

    /**
     * Uses RangeSlicer in place of pig's default Slicer to generate a few
     * values and count them.
     */
    @Test
    public void testUseRangeSlicer() throws ExecException, IOException {
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
        int numvals = 50;
        String query = "vals = foreach (group (load '/tmp/foo"
                + numvals
                + "'using org.apache.pig.test.RangeSlicer('50')) all) generate COUNT($1);";
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("vals");
        Tuple cur = it.next();
        Long val = (Long)cur.get(0);
        assertEquals(new Long(numvals), val);
    }
}
