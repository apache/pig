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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLoad {

    PigContext pc;
    PigServer[] servers;
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    
    @Before
    public void setUp() throws Exception {
        FileLocalizer.deleteTempFiles();
        servers = new PigServer[] { 
                    new PigServer(ExecType.MAPREDUCE, cluster.getProperties()),
                    new PigServer(ExecType.LOCAL, new Properties())
        };       
    }
        
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNextTuple() throws IOException {
        pc = servers[0].getPigContext();
        String curDir = System.getProperty("user.dir");
        String inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
            inpDir="/"+FileLocalizer.parseCygPath(inpDir, FileLocalizer.STYLE_WINDOWS);
        // copy passwd file to cluster and set that as the input location for the load
        Util.copyFromLocalToCluster(cluster, inpDir + "passwd", "passwd");
        FileSpec inpFSpec = new FileSpec("passwd", new FuncSpec(PigStorage.class.getName(), new String[]{":"}));
        POLoad ld = GenPhyOp.topLoadOp();
        ld.setLFile(inpFSpec);
        ld.setPc(pc);
        
        DataBag inpDB = DefaultBagFactory.getInstance().newDefaultBag();
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
    public void testLoadRemoteRel() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("test","/tmp/test");
        }
    }

    @Test
    public void testLoadRemoteAbs() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("/tmp/test","/tmp/test");
        }
    }

    @Test
    public void testLoadRemoteRelScheme() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("test","/tmp/test");
        }
    }

    @Test
    public void testLoadRemoteAbsScheme() throws Exception {
        pc = servers[0].getPigContext();
        checkLoadPath("hdfs:/tmp/test","/tmp/test");
    }

    @Test
    public void testLoadRemoteAbsAuth() throws Exception {
        pc = servers[0].getPigContext();
        checkLoadPath("hdfs://localhost:9000/test","/test");
    }

    @Test
    public void testLoadRemoteNormalize() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("/tmp/foo/../././","/tmp");
        }
    }

    @Test
    public void testGlobChars() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("t?s*","/tmp/t?s*");
        }
    }

    @Test
    public void testCommaSeparatedString() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("usr/pig/a,usr/pig/b","/tmp/usr/pig/a,/tmp/usr/pig/b");
        }
    }

    @Test
    public void testCommaSeparatedString2() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("t?s*,test","/tmp/t?s*,/tmp/test");
        }
    }

    @Test
    public void testCommaSeparatedString3() throws Exception {
        PigServer pig = servers[0];
        pc = pig.getPigContext();
        checkLoadPath("hdfs:/tmp/test,hdfs:/tmp/test2,hdfs:/tmp/test3","/tmp/test,/tmp/test2,/tmp/test3");
    }
    
    @Test
    public void testCommaSeparatedString4() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("usr/pig/{a,c},usr/pig/b","/tmp/usr/pig/{a,c},/tmp/usr/pig/b");
        }
    }

    @Test
    public void testCommaSeparatedString5() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("/usr/pig/{a,c},usr/pig/b","/usr/pig/{a,c},/tmp/usr/pig/b");
        }
    }
    
    @Test
    public void testCommaSeparatedString6() throws Exception {
        for (PigServer pig : servers) {
            pc = pig.getPigContext();
            checkLoadPath("usr/pig/{a,c},/usr/pig/b","/tmp/usr/pig/{a,c},/usr/pig/b");
        }
    }    

    private void checkLoadPath(String orig, String expected) throws Exception {
        checkLoadPath(orig, expected, false);
    }

    private void checkLoadPath(String orig, String expected, boolean isTmp) throws Exception {
        boolean[] multiquery = {true, false};
        for (boolean b : multiquery) {
            pc.getProperties().setProperty("opt.multiquery", "" + b);
                    
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
}
