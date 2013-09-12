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

import static org.apache.pig.newplan.logical.relational.LOTestHelper.newLOLoad;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestInputOutputMiniClusterFileValidator {
    private static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pig;
    private PigContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new PigContext(ExecType.MAPREDUCE, cluster.getProperties());
        ctx.connect() ;
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testMapReduceModeInputPositive() throws Throwable {
        String inputfile = createHadoopTempFile(ctx) ;
        String outputfile = createHadoopNonExistenceTempFile(ctx) ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile, ctx.getDfs()) ;

        InputOutputFileValidator executor = new InputOutputFileValidator(plan, ctx) ;
        executor.validate() ;
    }

    @Test
    public void testMapReduceModeInputNegative2() throws Throwable {
        String inputfile = createHadoopTempFile(ctx) ;
        String outputfile = createHadoopTempFile(ctx) ;

        LogicalPlan plan = genNewLoadStorePlan(inputfile, outputfile, ctx.getDfs()) ;

        InputOutputFileValidator executor = new InputOutputFileValidator(plan, ctx) ;
        try {
            executor.validate() ;
            fail("Excepted to fail.");
        } catch(Exception e) {
            //good
        }
    }

    /**
     * Testcase to ensure Input output validation allows store to a location
     * that does not exist when using {@link PigServer#store(String, String)}
     * @throws Exception
     */
    @Test
    public void testPigServerStore() throws Exception {
        String input = "input.txt";
        String output= "output.txt";
        String data[] = new String[] {"hello\tworld"};
            try {
                // reinitialize FileLocalizer for each mode
                // this is need for the tmp file creation as part of
                // PigServer.openIterator
                FileLocalizer.setInitialized(false);
                Util.deleteFile(pig.getPigContext(), input);
                Util.deleteFile(pig.getPigContext(), output);
                Util.createInputFile(pig.getPigContext(), input, data);
                pig.registerQuery("a = load '" + input + "';");
                pig.store("a", output);
                pig.registerQuery("b = load '" + output + "';");
                Iterator<Tuple> it = pig.openIterator("b");
                Tuple t = it.next();
                assertEquals("hello", t.get(0).toString());
                assertEquals("world", t.get(1).toString());
                assertFalse(it.hasNext());
            } finally {
                Util.deleteFile(pig.getPigContext(), input);
                Util.deleteFile(pig.getPigContext(), output);
            }

    }

    /**
     * Test case to test that Input output file validation catches the case
     * where the output file exists when using
     * {@link PigServer#store(String, String)}
     * @throws Exception
     */
    @Test(expected = PigException.class)
    public void testPigServerStoreNeg() throws Exception {
        String input = "input.txt";
        String output= "output.txt";
        String data[] = new String[] {"hello\tworld"};
             try {
                Util.deleteFile(pig.getPigContext(), input);
                Util.deleteFile(pig.getPigContext(), output);
                Util.createInputFile(pig.getPigContext(), input, data);
                Util.createInputFile(pig.getPigContext(), output, data);
                try {
                    pig.registerQuery("a = load '" + input + "';");
                    pig.store("a", output);
                    fail("Expected exception to be caught");
                } catch (Exception e) {
                    assertEquals(6000, LogUtils.getPigException(e).getErrorCode());
                    throw e;
                }
            } finally {
                Util.deleteFile(pig.getPigContext(), input);
                Util.deleteFile(pig.getPigContext(), output);
            }
        }


    @Test
    public void testValidationNeg() throws Throwable{

        PigServer pig = new PigServer(ExecType.MAPREDUCE,cluster.getProperties());
        try{
            pig.setBatchOn();
            pig.registerQuery("A = load 'inputfile' using PigStorage () as (a:int);");
            pig.registerQuery("store A into 'outfile' using "+DummyStorer.class.getName()+";");
            pig.executeBatch();
            assert false;
        }catch(Exception fe){
            assertTrue(fe instanceof FrontendException);
            PigException pe = LogUtils.getPigException(fe);
            assertTrue(pe instanceof FrontendException);
            assertEquals(1115, pe.getErrorCode());
            assertTrue(pe.getMessage().contains("Exception from DummyStorer."));
        }
    }


    private LogicalPlan genNewLoadStorePlan(String inputFile,
                                            String outputFile, DataStorage dfs)
                                        throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        FileSpec filespec1 =
            new FileSpec(inputFile, new FuncSpec("org.apache.pig.builtin.PigStorage")) ;
        FileSpec filespec2 =
            new FileSpec(outputFile, new FuncSpec("org.apache.pig.builtin.PigStorage"));
        LOLoad load = newLOLoad( filespec1, null, plan,
                ConfigurationUtil.toConfiguration(dfs.getConfiguration())) ;
        LOStore store = new LOStore(plan, filespec2, (StoreFuncInterface)PigContext.instantiateFuncFromSpec(filespec2.getFuncSpec()), null) ;

        plan.add(load) ;
        plan.add(store) ;

        plan.connect(load, store) ;

        return plan ;
    }

    private File generateTempFile() throws Throwable {
        File fp1 = File.createTempFile("file", ".txt") ;
        BufferedWriter bw = new BufferedWriter(new FileWriter(fp1)) ;
        bw.write("hohoho") ;
        bw.close() ;
        fp1.deleteOnExit() ;
        return fp1 ;
    }

    private String createHadoopTempFile(PigContext ctx) throws Throwable {

        File fp1 = generateTempFile() ;

        ElementDescriptor localElem =
            ctx.getLfs().asElement(fp1.getAbsolutePath());

        String path = fp1.getAbsolutePath();

        ElementDescriptor distribElem = ctx.getDfs().asElement(Util.removeColon(path)) ;

        localElem.copy(distribElem, null, false);

        return distribElem.toString();
    }

    private String createHadoopNonExistenceTempFile(PigContext ctx) throws Throwable {

        File fp1 = generateTempFile() ;

        String path = fp1.getAbsolutePath();

        ElementDescriptor distribElem = ctx.getDfs().asElement(Util.removeColon(path)) ;

        if (distribElem.exists()) {
            distribElem.delete() ;
        }

        return distribElem.toString();
    }

    public static class DummyStorer extends PigStorage{
    @Override
        public void checkSchema(ResourceSchema s) throws IOException {
            throw new FrontendException("Exception from DummyStorer.", 1115);
        }
    }
}
