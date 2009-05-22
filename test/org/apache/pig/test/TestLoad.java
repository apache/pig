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

import java.util.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.PigServer;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLoad extends junit.framework.TestCase {
    FileSpec inpFSpec;
    POLoad ld;
    PigContext pc;
    DataBag inpDB;
    String curDir;
    String inpDir;
    PigServer pig;
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    @Before
    public void setUp() throws Exception {
        curDir = System.getProperty("user.dir");
        inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
            inpDir="/"+FileLocalizer.parseCygPath(inpDir, FileLocalizer.STYLE_WINDOWS);
        inpFSpec = new FileSpec("file:" + inpDir + "passwd", new FuncSpec(PigStorage.class.getName(), new String[]{":"}));

        FileLocalizer.deleteTempFiles();
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pc = pig.getPigContext();
        
        ld = GenPhyOp.topLoadOp();
        ld.setLFile(inpFSpec);
        ld.setPc(pc);
        
        inpDB = DefaultBagFactory.getInstance().newDefaultBag();
        BufferedReader br = new BufferedReader(new FileReader("test/org/apache/pig/test/data/InputFiles/passwd"));
        
        for(String line = br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split(":",-1);
            Tuple t = new DefaultTuple();
            for (String fld : flds) {
                t.append((fld.compareTo("")!=0 ? new DataByteArray(fld.getBytes()) : null));
            }
            inpDB.add(t);
        }
    }
    
    

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNextTuple() throws ExecException {
        Tuple t=null;
        int size = 0;
        for(Result res = ld.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=ld.getNext(t)){
            assertEquals(true, TestHelper.bagContains(inpDB, (Tuple)res.result));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
    }

    @Test
    public void testLoadLocalRel() throws Exception {
        checkLoadPath("file:test/org/apache/pig/test/data/passwd", "", true);
    }

    @Test
    public void testLoadLocalAbs() throws Exception {
    	String filename = curDir + File.separatorChar+"test/org/apache/pig/test/data/passwd";
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
        {
            filename="/"+FileLocalizer.parseCygPath(filename, FileLocalizer.STYLE_WINDOWS);
            filename=Util.encodeEscape(filename);
        }
        checkLoadPath("file:"+filename, "", true);
    }

    @Test
    public void testLoadRemoteRel() throws Exception {
        checkLoadPath("test","/tmp/test");
    }

    @Test
    public void testLoadRemoteAbs() throws Exception {
        checkLoadPath("/tmp/test","/tmp/test");
    }

    @Test
    public void testLoadRemoteRelScheme() throws Exception {
        checkLoadPath("test","/tmp/test");
    }

    @Test
    public void testLoadRemoteAbsScheme() throws Exception {
        checkLoadPath("hdfs:/tmp/test","/tmp/test");
    }

    @Test
    public void testLoadRemoteAbsAuth() throws Exception {
        checkLoadPath("hdfs://localhost:9000/test","/test");
    }

    @Test
    public void testLoadRemoteNormalize() throws Exception {
        checkLoadPath("/tmp/foo/../././","/tmp");
    }

    @Test
    public void testGlobChars() throws Exception {
        checkLoadPath("t?s*","/tmp/t?s*");
    }

    private void checkLoadPath(String orig, String expected) throws Exception {
        checkLoadPath(orig, expected, false);
    }

    private void checkLoadPath(String orig, String expected, boolean isTmp) throws Exception {
        pc.getProperties().setProperty("opt.multiquery",""+true);
                
        DataStorage dfs = pc.getDfs();
        dfs.setActiveContainer(dfs.asContainer("/tmp"));
        Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
        Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
        Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
        Map<String, String> fileNameMap = new HashMap<String, String>();
        
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pc);
        
        String query = "a = load '"+orig+"';";
        LogicalPlan lp = builder.parse("Test-Load",
                                       query,
                                       aliases,
                                       logicalOpTable,
                                       aliasOp,
                                       fileNameMap);
        Assert.assertTrue(lp.size()>0);
        LogicalOperator op = lp.getRoots().get(0);
        
        Assert.assertTrue(op instanceof LOLoad);
        LOLoad load = (LOLoad)op;

        String p = load.getInputFile().getFileName();
        p = p.replaceAll("hdfs://[0-9a-zA-Z:\\.]*/","/");

        if (isTmp) {
            Assert.assertTrue(p.matches("/tmp.*"));
        } else {
            Assert.assertEquals(p, expected);
        }
    }
}
