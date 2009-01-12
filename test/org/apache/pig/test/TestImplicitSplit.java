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


import static org.apache.pig.test.utils.TypeCheckingTestUtil.printMessageCollector;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.printTypeGraph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckingValidator;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestImplicitSplit extends TestCase{
    private PigServer pigServer;
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testImplicitSplit() throws Exception{
        int LOOP_SIZE = 20;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = filter A by $0<=10;");
        pigServer.registerQuery("C = filter A by $0>10;");
        pigServer.registerQuery("D = union B,C;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void testImplicitSplitInCoGroup() throws Exception {
        // this query is similar to the one reported in JIRA - PIG-537
        // Create input file
        File inputA = Util.createInputFile("tmp", "", 
                new String[] {"a:1", "b:2", "b:20", "c:3", "c:30"});
        File inputB = Util.createInputFile("tmp", "", 
                new String[] {"a:first", "b:second", "c:third"});
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(inputA.toString()) + 
                "' using PigStorage(':') as (name:chararray, marks:int);");
        pigServer.registerQuery("b = load 'file:" + Util.encodeEscape(inputA.toString()) + 
                "' using PigStorage(':') as (name:chararray, rank:chararray);");
        pigServer.registerQuery("c = cogroup a by name, b by name;");
        pigServer.registerQuery("d = foreach c generate group, FLATTEN(a.marks) as newmarks;");
        pigServer.registerQuery("e = cogroup a by marks, d by newmarks;");
        pigServer.registerQuery("f = foreach e generate group, flatten(a), flatten(d);");
        HashMap<Integer, Object[]> results = new HashMap<Integer, Object[]>();
        results.put(1, new Object[] { "a", 1, "a", 1 });
        results.put(2, new Object[] { "b", 2, "b", 2 });
        results.put(3, new Object[] { "c", 3, "c", 3 });
        results.put(20, new Object[] { "b", 20, "b", 20 });
        results.put(30, new Object[] { "c", 30, "c", 30 });
        
        Iterator<Tuple> it = pigServer.openIterator("f");
        while(it.hasNext()) {
            Tuple t = it.next();
            System.err.println("Tuple:" + t);
            Integer group = (Integer)t.get(0);
            Object[] groupValues = results.get(group);
            for(int i = 0; i < 4; i++) {
                assertEquals(groupValues[i], t.get(i+1));    
            }
        }
    }
    
    @Test
    public void testImplicitSplitInCoGroup2() throws Exception {
        // this query is similar to the one reported in JIRA - PIG-537
        LogicalPlanTester planTester = new LogicalPlanTester();
        planTester.buildPlan("a = load 'file1' using PigStorage(':') as (name:chararray, marks:int);");
        planTester.buildPlan("b = load 'file2' using PigStorage(':') as (name:chararray, rank:chararray);");
        planTester.buildPlan("c = cogroup a by name, b by name;");
        planTester.buildPlan("d = foreach c generate group, FLATTEN(a.marks) as newmarks;");
        planTester.buildPlan("e = cogroup a by marks, d by newmarks;");
        LogicalPlan plan = planTester.buildPlan("f = foreach e generate group, flatten(a), flatten(d);");
        
        // Set the logical plan values correctly in all the operators
        PlanSetter ps = new PlanSetter(plan);
        ps.visit();
        
        // run through validator
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }

        // this will run ImplicitSplitInserter
        TestLogicalOptimizer.optimizePlan(plan);
        
        // get Schema of leaf and compare:
        Schema expectedSchema = Util.getSchemaFromString("grp: int,A::username: chararray,A::marks: int,AB::group: chararray,AB::newmarks: int");
        assertTrue(Schema.equals(expectedSchema, plan.getLeaves().get(0).getSchema(),false, true));
    }
}
