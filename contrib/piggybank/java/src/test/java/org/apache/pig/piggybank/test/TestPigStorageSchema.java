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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPigStorageSchema {

    protected ExecType execType = ExecType.MAPREDUCE;
    static MiniCluster cluster = MiniCluster.buildCluster();
    static PigServer pig;

    PigContext pigContext = new PigContext(ExecType.MAPREDUCE, new Properties());
    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    Map<String, String> fileNameMap = new HashMap<String, String>();

    @Before
    public void setUp() throws Exception {
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
    public void tearDown() throws Exception {
        for (String f : new String[] {"originput", "aout", "originput2",
                "bout", ".pig_schema.bout", ".pig_schema.aout", "cout", ".pig_schema.cout",
        ".pig_schema"}) {
            if (FileLocalizer.fileExists(f, pig.getPigContext())) {
                FileLocalizer.delete(f, pig.getPigContext());
        }
    }
        pig.shutdown();
    }
    
    @Test
    public void testPigStorageSchema() throws Exception {
        pigContext.connect();
        String query = "a = LOAD 'originput' using org.apache.pig.piggybank.storage.PigStorageSchema() " +
        "as (f1:chararray, f2:int);";
        pig.registerQuery(query);
        Schema origSchema = pig.dumpSchema("a");
        pig.registerQuery("STORE a into 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        
        // aout now has a schema. 

        // Verify that loading a-out with no given schema produces 
        // the original schema.
        
        pig.registerQuery("b = LOAD 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        Schema genSchema = pig.dumpSchema("b");
        Assert.assertTrue("generated schema equals original" ,
                Schema.equals(genSchema, origSchema, true, false));
        
        // Verify that giving our own schema works
        String [] aliases ={"foo", "bar"};
        byte[] types = {DataType.INTEGER, DataType.LONG};
        Schema newSchema = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        pig.registerQuery("c = LOAD 'aout' using org.apache.pig.piggybank.storage.PigStorageSchema() "+
        "as (foo:int, bar:long);");
        Schema newGenSchema = pig.dumpSchema("c");
        Assert.assertTrue("explicit schema overrides metadata",
                Schema.equals(newSchema, newGenSchema, true, false));
        
    }
    
    @Test
    public void testSchemaConversion() throws Exception {   
 
        Util.createInputFile(cluster, "originput2", 
                new String[] {"1", "2", "3", "2",
                              "5", "5", "8", "8",
                              "8", "9"});
        
        pig.registerQuery("A = LOAD 'originput2' using org.apache.pig.piggybank.storage.PigStorageSchema() " +
        "as (f:int);");
        pig.registerQuery("B = group A by f;");
        Schema origSchema = pig.dumpSchema("B");
        ResourceSchema rs1 = new ResourceSchema(origSchema);
        pig.registerQuery("STORE B into 'bout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        
        pig.registerQuery("C = LOAD 'bout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        Schema genSchema = pig.dumpSchema("C");
        ResourceSchema rs2 = new ResourceSchema(genSchema);
        Assert.assertTrue("generated schema equals original" , ResourceSchema.equals(rs1, rs2));
        
        pig.registerQuery("C1 = LOAD 'bout' as (a0:int, A: {t: (f:int) } );");
        pig.registerQuery("D = foreach C1 generate a0, SUM(A);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { 
                        "(1,1L)",
                        "(2,4L)",
                        "(3,3L)",
                        "(5,10L)",
                        "(8,24L)",
                        "(9,9L)"
                });
        
        Iterator<Tuple> iter = pig.openIterator("D");
        int counter = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }
        
        Assert.assertEquals(expectedResults.size(), counter);
    }
    
    @Test
    public void testSchemaConversion2() throws Exception {   
 
        pig.registerQuery("A = LOAD 'originput' using org.apache.pig.piggybank.storage.PigStorageSchema(',') " +
        "as (f1:chararray, f2:int);");
        pig.registerQuery("B = group A by f1;");
        Schema origSchema = pig.dumpSchema("B");
        ResourceSchema rs1 = new ResourceSchema(origSchema);
        pig.registerQuery("STORE B into 'cout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        
        pig.registerQuery("C = LOAD 'cout' using org.apache.pig.piggybank.storage.PigStorageSchema();");
        Schema genSchema = pig.dumpSchema("C");
        ResourceSchema rs2 = new ResourceSchema(genSchema);
        Assert.assertTrue("generated schema equals original" , ResourceSchema.equals(rs1, rs2));
        
        pig.registerQuery("C1 = LOAD 'cout' as (a0:chararray, A: {t: (f1:chararray, f2:int) } );");
        pig.registerQuery("D = foreach C1 generate a0, SUM(A.f2);");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { 
                        "('A',23L)",
                        "('B',7L)",
                        "('C',11L)",
                        "('D',10L)"
                });
        
        Iterator<Tuple> iter = pig.openIterator("D");
        int counter = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }
        
        Assert.assertEquals(expectedResults.size(), counter);
    }
 
    /**
     * See PIG-1830
     * @throws IOException
     */
    @Test
    public void testByteArrayConversion() throws IOException {
        Util.createInputFile(cluster, "originput2",
                new String[] {"peter\t1", "samir\t2", "michael\t4",
                "peter\t2", "peter\t4", "samir\t1"
        });
        Util.createInputFile(cluster, ".pig_schema",
                new String[] {
                "{\"fields\":[{\"name\":\"name\",\"type\":55,\"schema\":null," +
                "\"description\":\"autogenerated from Pig Field Schema\"}," +
                "{\"name\":\"val\",\"type\":10,\"schema\":null,\"description\":"+
                "\"autogenerated from Pig Field Schema\"}],\"version\":0," +
                "\"sortKeys\":[],\"sortKeyOrders\":[]}"
        });
        pig.registerQuery("Events = LOAD 'originput2' USING org.apache.pig.piggybank.storage.PigStorageSchema();");
        pig.registerQuery("Sessions = GROUP Events BY name;");
        Iterator<Tuple> sessions = pig.openIterator("Sessions");
        while (sessions.hasNext()) {
            System.out.println(sessions.next());
}


    }
}
