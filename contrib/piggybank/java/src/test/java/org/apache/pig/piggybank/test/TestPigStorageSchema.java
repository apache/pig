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


package org.apache.pig.piggybank.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestPigStorageSchema extends TestCase {

    protected ExecType execType = ExecType.MAPREDUCE;

    PigContext pigContext = new PigContext(ExecType.MAPREDUCE, new Properties());
    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    Map<String, String> fileNameMap = new HashMap<String, String>();

    MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        String origPath = FileLocalizer.fullPath("originput", pig.getPigContext()); 
        if (FileLocalizer.fileExists(origPath, pig.getPigContext())) {
            FileLocalizer.delete(origPath, pig.getPigContext());
        }
        Util.createInputFile(cluster, "originput", 
                new String[] {"A,1", "B,2", "C,3", "D,2",
                              "A,5", "B,5", "C,8", "A,8",
                              "D,8", "A,9"});

    }
    
    @After
    @Override
    protected void tearDown() throws Exception {
        Util.deleteFile(cluster, "originput");
        String aoutPath = FileLocalizer.fullPath("aout", pig.getPigContext()); 
        if (FileLocalizer.fileExists(aoutPath, pig.getPigContext())) {
            FileLocalizer.delete(aoutPath, pig.getPigContext());
        }
    }
    
    @Test
    public void testPigStorageSchema() throws Exception {
        pigContext.connect();
        String query = "a = LOAD 'originput' using org.apache.pig.piggybank.storage.PigStorageSchema() as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        Schema origSchema = pig.dumpSchema("a");
        pig.registerQuery("STORE a into 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        
        // aout now has a schema. 

        // Verify that loading a-out with no given schema produces 
        // the original schema.
        
        pig.registerQuery("b = LOAD 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        Schema genSchema = pig.dumpSchema("b");
        assertTrue("generated schema equals original" , Schema.equals(genSchema, origSchema, true, false));
        
        // Verify that giving our own schema works
        String [] aliases ={"foo", "bar"};
        byte[] types = {DataType.INTEGER, DataType.LONG};
        Schema newSchema = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        pig.registerQuery("c = LOAD 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema() as (foo:int, bar:long);");
        Schema newGenSchema = pig.dumpSchema("c");
        assertTrue("explicit schema overrides metadata", Schema.equals(newSchema, newGenSchema, true, false));
        
    }
}
