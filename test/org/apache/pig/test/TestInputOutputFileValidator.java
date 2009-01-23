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

import java.io.* ;
import java.util.Properties;

import org.apache.pig.ExecType; 
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;

import org.apache.pig.impl.logicalLayer.validators.* ;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType; 
import org.junit.Test;
import junit.framework.TestCase;

public class TestInputOutputFileValidator extends TestCase {
    
    
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    @Test
    public void testLocalModeInputPositive() throws Throwable {
        
        PigContext ctx = new PigContext(ExecType.LOCAL, new Properties()) ;
        ctx.connect() ;
        
        String inputfile = generateTempFile().getAbsolutePath() ;
        String outputfile = generateNonExistenceTempFile().getAbsolutePath() ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile) ;        
        
        CompilationMessageCollector collector = new CompilationMessageCollector() ;        
        LogicalPlanValidationExecutor executor = new LogicalPlanValidationExecutor(plan, ctx) ;
        executor.validate(plan, collector) ;
        
        assertFalse(collector.hasError()) ;

    }
    
       
    @Test
    public void testLocalModeNegative2() throws Throwable {
        
        PigContext ctx = new PigContext(ExecType.LOCAL, new Properties()) ;
        ctx.connect() ;
        
        String inputfile = generateTempFile().getAbsolutePath() ;
        String outputfile = generateTempFile().getAbsolutePath() ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile) ;        
        
        CompilationMessageCollector collector = new CompilationMessageCollector() ;        
        LogicalPlanValidationExecutor executor = new LogicalPlanValidationExecutor(plan, ctx) ;
        try {
            executor.validate(plan, collector) ;
            fail("Expected to fail.");
        } catch (Exception pve) {
            //good
        }
        
        assertEquals(collector.size(), 3) ;
        for(int i = 0; i < collector.size(); ++i) {
            assertEquals(collector.get(i).getMessageType(), MessageType.Error) ;
        }        

    }
    
        
    @Test
    public void testMapReduceModeInputPositive() throws Throwable {
        
        PigContext ctx = new PigContext(ExecType.MAPREDUCE, cluster.getProperties()) ;       
        ctx.connect() ;
        
        String inputfile = createHadoopTempFile(ctx) ;
        String outputfile = createHadoopNonExistenceTempFile(ctx) ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile) ;                     
        
        CompilationMessageCollector collector = new CompilationMessageCollector() ;        
        LogicalPlanValidationExecutor executor = new LogicalPlanValidationExecutor(plan, ctx) ;
        executor.validate(plan, collector) ;
            
        assertFalse(collector.hasError()) ;

    }
    
       
    @Test
    public void testMapReduceModeInputNegative2() throws Throwable {
        
        PigContext ctx = new PigContext(ExecType.MAPREDUCE, cluster.getProperties()) ;       
        ctx.connect() ;
        
        String inputfile = createHadoopTempFile(ctx) ;
        String outputfile = createHadoopTempFile(ctx) ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile) ;                     
        
        CompilationMessageCollector collector = new CompilationMessageCollector() ;        
        LogicalPlanValidationExecutor executor = new LogicalPlanValidationExecutor(plan, ctx) ;
        try {
            executor.validate(plan, collector) ;
            fail("Excepted to fail.");
        } catch(Exception e) {
            //good
        }
        
        assertEquals(collector.size(), 3) ;
        for(int i = 0; i < collector.size(); ++i) {
            assertEquals(collector.get(i).getMessageType(), MessageType.Error) ;
        }       

    }
    
        
    private LogicalPlan genNewLoadStorePlan(String inputFile,
                                            String outputFile) 
                                        throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        FileSpec filespec1 =
            new FileSpec(inputFile, new FuncSpec("org.apache.pig.builtin.PigStorage")) ;
        FileSpec filespec2 =
            new FileSpec(outputFile, new FuncSpec("org.apache.pig.builtin.PigStorage"));
        LOLoad load = new LOLoad(plan, genNewOperatorKeyId(), filespec1, null, null, true) ;       
        LOStore store = new LOStore(plan, genNewOperatorKeyId(), filespec2) ;
        
        plan.add(load) ;
        plan.add(store) ;
        
        plan.connect(load, store) ;     
        
        return plan ;    
    }
    
    private OperatorKey genNewOperatorKeyId() {
        long newId = NodeIdGenerator.getGenerator().getNextNodeId("scope") ;
        return new OperatorKey("scope", newId) ;
    }   

    private File generateTempFile() throws Throwable {
        File fp1 = File.createTempFile("file", ".txt") ;
        BufferedWriter bw = new BufferedWriter(new FileWriter(fp1)) ;
        bw.write("hohoho") ;
        bw.close() ;
        fp1.deleteOnExit() ;
        return fp1 ;
    }
    
    private File generateNonExistenceTempFile() throws Throwable {
        File fp1 = File.createTempFile("file", ".txt") ;
        fp1.delete() ;
        return fp1 ;
    }
    
    private String createHadoopTempFile(PigContext ctx) throws Throwable {
        
        File fp1 = generateTempFile() ;
                
        ElementDescriptor localElem =
            ctx.getLfs().asElement(fp1.getAbsolutePath());           
            
        String path = fp1.getAbsolutePath();
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            path = FileLocalizer.parseCygPath(path, FileLocalizer.STYLE_UNIX);
            
        ElementDescriptor distribElem = ctx.getDfs().asElement(path) ;
    
        localElem.copy(distribElem, null, false);
            
        return distribElem.toString();
    }
    
    private String createHadoopNonExistenceTempFile(PigContext ctx) throws Throwable {
        
        File fp1 = generateTempFile() ;         
         
        String path = fp1.getAbsolutePath();
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            path = FileLocalizer.parseCygPath(path, FileLocalizer.STYLE_UNIX);
        
        ElementDescriptor distribElem = ctx.getDfs().asElement(path) ;
        
        if (distribElem.exists()) {
            distribElem.delete() ;
        }   
            
        return distribElem.toString();
    }
}
