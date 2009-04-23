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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.local.executionengine.LocalPigLauncher;
import org.apache.pig.backend.local.executionengine.LocalPOStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLocalJobSubmission extends junit.framework.TestCase{
    static PigContext pc;
    String ldFile;
    String expFile;
    PhysicalPlan php = new PhysicalPlan();
    String stFile;
    String grpName;
    String curDir;
    String outDir;
    String inpDir;
    String golDir;
    Random r = new Random();
    
    static {
//        MiniCluster cluster = MiniCluster.buildCluster();
        pc = new PigContext();
        try {
            pc.connect();
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        GenPhyOp.setPc(pc);
    }
    
    
    
    /*@Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(outDir!=null && new File(outDir).exists())
            rmrf(outDir);
    }*/

    @Before
    public void setUp() throws Exception{
        curDir = System.getProperty("user.dir");
        inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        outDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/OutputFiles/";
        golDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/GoldenFiles/";
        inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
            inpDir="/"+FileLocalizer.parseCygPath(inpDir, FileLocalizer.STYLE_WINDOWS);
        
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
            golDir="/"+FileLocalizer.parseCygPath(golDir, FileLocalizer.STYLE_WINDOWS);
        
        File f = new File(outDir);
        
        if ((System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))
            outDir="/"+FileLocalizer.parseCygPath(outDir, FileLocalizer.STYLE_WINDOWS);
        
        boolean didMakeDir = f.mkdirs();
        /*if(!didMakeDir)
            throw new Exception("Could not Create Directory " + outDir);*/
    }
    
    private void rmrf(String dir) throws Exception{
        File f = new File(dir);
        File[] ls = f.listFiles();
        for (File file : ls) {
            if(file.isFile())
                file.delete();
            else
                rmrf(file.getPath());
        }
        boolean isDeleted = f.delete();
        if(!isDeleted)
            throw new Exception("Could not Delete Directory" + dir);
    }
    
    @After
    public void tearDown() throws Exception {
        rmrf(outDir);
    }
    
    private void generateInput(int numTuples) throws Exception{
        
        DataBag inpDb = GenRandomData.genRandSmallTupDataBag(r, numTuples, 1000);
        
        POProject proj = new POProject(new OperatorKey("", r.nextLong()));
        Tuple t = new DefaultTuple();
        t.append(inpDb);
        proj.attachInput(t);
        proj.setColumn(0);
        proj.setOverloaded(true);
        proj.setResultType(DataType.TUPLE);
        
        List<PhysicalOperator> inps = new ArrayList<PhysicalOperator>();
        inps.add(proj);
        
        POStore str = new POStore(new OperatorKey("", r.nextLong()));
        
        FileSpec fSpec = new FileSpec(ldFile, new FuncSpec(PigStorage.class.getName()));
        
        str.setSFile(fSpec);
        str.setStoreImpl(new LocalPOStoreImpl(pc));

        PhysicalPlan pp = new PhysicalPlan();
        pp.add(proj);
        pp.add(str);
        pp.connect(proj,str);
        
        new LocalPigLauncher().launchPig(pp, "TestLocalJobSubmission", pc);
    }
    
    /*private void setUp1(boolean gen) throws Exception {
        
        ldFile = "file:" + inpDir + "jsTst1.txt";
        expFile = ldFile;
        stFile = "file:" + outDir + "jsTst1";
        grpName = "jobSubTst1";
        
        if(gen){
            generateInput(100);
            return;
        }
        
        FileSpec LFSpec = new FileSpec(ldFile,PigStorage.class.getName());
        FileSpec SFSpec = new FileSpec(stFile,PigStorage.class.getName());

        POLoad ld = new POLoad(new OperatorKey("", r.nextLong()));
        POStore st = new POStore(new OperatorKey("", r.nextLong()));
        ld.setPc(pc);
        ld.setLFile(LFSpec);
        st.setPc(pc);
        st.setSFile(SFSpec);
        
        php.add(ld);
        php.add(st);
        php.connect(ld, st);
     }

    @Test
    public void testCompile1() throws Exception {
        boolean gen = false;

        setUp1(gen);
        
        if(gen)
            return;

        submit();
        
        assertEquals(true, FileLocalizer.fileExists(stFile, pc));
        
        FileSpec fSpecExp = new FileSpec(expFile,PigStorage.class.getName());
        FileSpec fSpecAct = new FileSpec(stFile,PigStorage.class.getName());
        
        assertEquals(true, TestHelper.areFilesSame(fSpecExp, fSpecAct, pc));
    }
    
    private void setUp2(boolean gen) throws Exception {
        ldFile = "file:" + inpDir + "jsTst2.txt";
        expFile = ldFile;
        stFile = "file:" + outDir + "jsTst2";
        grpName = "jobSubTst2";
        
        if(gen){
            generateInput(1000);
            return;
        }
        
        FileSpec LFSpec = new FileSpec(ldFile,PigStorage.class.getName());
        FileSpec SFSpec = new FileSpec(stFile,PigStorage.class.getName());

        POLoad ld = new POLoad(new OperatorKey("", r.nextLong()));
        POStore st = new POStore(new OperatorKey("", r.nextLong()));
        ld.setPc(pc);
        ld.setLFile(LFSpec);
        st.setPc(pc);
        st.setSFile(SFSpec);
        
        php.add(ld);
        php.add(st);
        php.connect(ld, st);
     }

    @Test
    public void testCompile2() throws Exception {
        boolean gen = false;

        setUp2(gen);
        
        if(gen)
            return;

        submit();
        
        assertEquals(true, FileLocalizer.fileExists(stFile, pc));
        
        FileSpec fSpecExp = new FileSpec(expFile,PigStorage.class.getName());
        FileSpec fSpecAct = new FileSpec(stFile,PigStorage.class.getName());
        
        assertEquals(true, TestHelper.areFilesSame(fSpecExp, fSpecAct, pc));
    }
    
    private void setUp3(boolean gen) throws Exception {
        ldFile = "file:" + inpDir + "jsTst1.txt";
        expFile = "file:" + golDir + "jsTst3";
        stFile = "file:" + outDir + "jsTst3";
        grpName = "jobSubTst3";
        
        if(gen){
            generateInput(1000);
            return;
        }
        
        FileSpec LFSpec = new FileSpec(ldFile,PigStorage.class.getName());
        FileSpec SFSpec = new FileSpec(stFile,PigStorage.class.getName());

        POLoad ld = new POLoad(new OperatorKey("", r.nextLong()));
        POStore st = new POStore(new OperatorKey("", r.nextLong()));
        ld.setPc(pc);
        ld.setLFile(LFSpec);
        st.setPc(pc);
        st.setSFile(SFSpec);
        
        int[] flds = {0,1};
        Tuple sample = new DefaultTuple();
        sample.append(new String("S"));
        sample.append(new Integer("10"));
        
        POForEach fe = GenPhyOp.topForEachOPWithPlan(flds , sample);
        
        POFilter fl = GenPhyOp.topFilterOpWithProj(1, 500, GenPhyOp.LT);
        
        php.add(ld);
        php.add(fe);
        php.connect(ld, fe);
        
        php.add(fl);
        php.connect(fe, fl);
        
        php.add(st);
        php.connect(fl, st);
     }

    @Test
    public void testCompile3() throws Exception {
        boolean gen = false;

        setUp3(gen);
        
        if(gen)
            return;

        submit();
        
        assertEquals(true, FileLocalizer.fileExists(stFile, pc));
        
        FileSpec fSpecExp = new FileSpec(expFile,PigStorage.class.getName()+"(',')");
        FileSpec fSpecAct = new FileSpec(stFile,PigStorage.class.getName());
        
        assertEquals(true, TestHelper.areFilesSame(fSpecExp, fSpecAct, pc));
    }
    
    private void setUp4(boolean gen) throws Exception {
        ldFile = "file:" + inpDir + "jsTst1.txt";
        expFile = "file:" + golDir + "jsTst4";
        stFile = "file:" + outDir + "jsTst4";
        grpName = "jobSubTst4";
        
        if(gen){
            generateInput(1000);
            return;
        }
        
        FileSpec LFSpec = new FileSpec(ldFile,PigStorage.class.getName());
        FileSpec SFSpec = new FileSpec(stFile,PigStorage.class.getName());

        POLoad ld = new POLoad(new OperatorKey("", r.nextLong()));
        POStore st = new POStore(new OperatorKey("", r.nextLong()));
        ld.setPc(pc);
        ld.setLFile(LFSpec);
        st.setPc(pc);
        st.setSFile(SFSpec);
        
        POSplit spl = GenPhyOp.topSplitOp();
        POFilter fl1 = GenPhyOp.topFilterOpWithProjWithCast(1, 200, GenPhyOp.LT);
        POFilter fl2 = GenPhyOp.topFilterOpWithProjWithCast(1, 800, GenPhyOp.GT);
        
        POUnion un = GenPhyOp.topUnionOp();
        
        php.add(ld);
        php.add(spl);
        php.connect(ld, spl);
        
        php.add(fl1);
        php.connect(spl, fl1);
        
        php.add(fl2);
        php.connect(spl, fl2);
        
        php.add(un);
        php.connect(fl1, un);
        php.connect(fl2, un);
        
        php.add(st);
        php.connect(un, st);
     }

    @Test
    public void testCompile4() throws Exception {
        boolean gen = false;

        setUp4(gen);
        
        if(gen)
            return;
        
        submit();
        
        assertEquals(true, FileLocalizer.fileExists(stFile, pc));
        
        FileSpec fSpecExp = new FileSpec(expFile, new FuncSpec(PigStorage.class.getName(), new String[]{","}));
        FileSpec fSpecAct = new FileSpec(stFile, new FuncSpec(PigStorage.class.getName()));
        
        assertEquals(true, TestHelper.areFilesSame(fSpecExp, fSpecAct, pc));
        
    }*/
    
    private void setUp5(boolean gen) throws Exception {
        ldFile = "file:" + inpDir + "jsTst5.txt";
        expFile = ldFile;
        stFile = "file:" + outDir + "jsTst5";
        grpName = "jobSubTst5";
        
        if(gen){
            generateInput(1000);
            return;
        }
        
        FileSpec LFSpec = new FileSpec(ldFile, new FuncSpec(PigStorage.class.getName(), new String[]{","}));
        FileSpec SFSpec = new FileSpec(stFile, new FuncSpec(PigStorage.class.getName()));

        POLoad ld = new POLoad(new OperatorKey("", r.nextLong()), true);
        POStore st = new POStore(new OperatorKey("", r.nextLong()));
        ld.setPc(pc);
        ld.setLFile(LFSpec);
        st.setSFile(SFSpec);
        
        Tuple sample = new DefaultTuple();
        sample.append("S");
        sample.append(1);
        POLocalRearrange lr = GenPhyOp.topLocalRearrangeOPWithPlan(0, 1, sample);
        
        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        
        POPackage pk = GenPhyOp.topPackageOp();
        pk.setKeyType(DataType.INTEGER);
        pk.setNumInps(1);
        boolean[] inner = {false}; 
        pk.setInner(inner);
        
        POForEach fe = GenPhyOp.topForEachOPWithPlan(1);
        
        php.add(ld);
        php.add(lr);
        php.connect(ld, lr);
        
        php.add(gr);
        php.connect(lr, gr);
        
        php.add(pk);
        php.connect(gr, pk);
        
        php.add(fe);
        php.connect(pk, fe);
        
        php.add(st);
        php.connect(fe, st);
     }

    @Test
    public void testCompile5() throws Exception {
        boolean gen = false;

        setUp5(gen);
        
        if(gen)
            return;
        
        submit();
        
        assertEquals(true, FileLocalizer.fileExists(stFile, pc));
        
        FileSpec fSpecExp = new FileSpec(expFile,
                        new FuncSpec(PigStorage.class.getName()+"(',')"));

        FileSpec fSpecAct = new FileSpec(stFile,
                                new FuncSpec(PigStorage.class.getName()));
        
        assertEquals(true, TestHelper.areFilesSame(fSpecExp, fSpecAct, pc));
        
    }
    
    private void submit() throws Exception{
        assertEquals(true, FileLocalizer.fileExists(ldFile, pc));
        MapReduceLauncher ll = new MapReduceLauncher();
        ll.launchPig(php, grpName, pc);  
    }
}
