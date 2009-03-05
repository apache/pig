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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.backend.local.executionengine.LocalPigLauncher;
import org.apache.pig.backend.local.executionengine.LocalPOStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStore extends junit.framework.TestCase {
    POStore st;
    FileSpec fSpec;
    DataBag inpDB;
    static MiniCluster cluster = MiniCluster.buildCluster();
    PigContext pc;
    POProject proj;
    
    @Before
    public void setUp() throws Exception {
        st = GenPhyOp.topStoreOp();
        fSpec = new FileSpec("file:/tmp/storeTest.txt",
                      new FuncSpec(PigStorage.class.getName(), new String[]{":"}));
        st.setSFile(fSpec);
        pc = new PigContext();
        pc.connect();
        st.setStoreImpl(new LocalPOStoreImpl(pc));
        
        proj = GenPhyOp.exprProject();
        proj.setColumn(0);
        proj.setResultType(DataType.TUPLE);
        proj.setOverloaded(true);
        List<PhysicalOperator> inps = new ArrayList<PhysicalOperator>();
    }

    @After
    public void tearDown() throws Exception {
    }

    private boolean store() throws Exception {
        PhysicalPlan pp = new PhysicalPlan();
        pp.add(proj);
        pp.add(st);
        pp.connect(proj, st);
        return new LocalPigLauncher().launchPig(pp, "TestStore", pc);
    }

    @Test
    public void testStore() throws Exception {
        inpDB = GenRandomData.genRandSmallTupDataBag(new Random(), 10, 100);
        Tuple t = new DefaultTuple();
        t.append(inpDB);
        proj.attachInput(t);
        assertTrue(store());
        
        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/storeTest.txt"));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split(":",-1);
            t = new DefaultTuple();
            t.append(flds[0].compareTo("")!=0 ? flds[0] : null);
            t.append(flds[1].compareTo("")!=0 ? Integer.parseInt(flds[1]) : null);
            
            System.err.println("Simple data: ");
            System.err.println(line);
            System.err.println("t: ");
            System.err.println(t);
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
        FileLocalizer.delete(fSpec.getFileName(), pc);
    }

    @Test
    public void testStoreComplexData() throws Exception {
        inpDB = GenRandomData.genRandFullTupTextDataBag(new Random(), 10, 100);
        Tuple t = new DefaultTuple();
        t.append(inpDB);
        proj.attachInput(t);
        assertTrue(store());
        PigStorage ps = new PigStorage(":");
        
        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/storeTest.txt"));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split(":",-1);
            t = new DefaultTuple();
            t.append(flds[0].compareTo("")!=0 ? ps.bytesToBag(flds[0].getBytes()) : null);
            t.append(flds[1].compareTo("")!=0 ? ps.bytesToCharArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.bytesToMap(flds[7].getBytes()) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.bytesToTuple(flds[8].getBytes()) : null);
            
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
        FileLocalizer.delete(fSpec.getFileName(), pc);
    }

    @Test
    public void testStoreComplexDataWithNull() throws Exception {
        Tuple inputTuple = GenRandomData.genRandSmallBagTextTupleWithNulls(new Random(), 10, 100);
        inpDB = DefaultBagFactory.getInstance().newDefaultBag();
        inpDB.add(inputTuple);
        Tuple t = new DefaultTuple();
        t.append(inpDB);
        proj.attachInput(t);
        assertTrue(store());
        PigStorage ps = new PigStorage(":");
        
        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/storeTest.txt"));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            System.err.println("Complex data: ");
            System.err.println(line);
            String[] flds = line.split(":",-1);
            t = new DefaultTuple();
            t.append(flds[0].compareTo("")!=0 ? ps.bytesToBag(flds[0].getBytes()) : null);
            t.append(flds[1].compareTo("")!=0 ? ps.bytesToCharArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.bytesToMap(flds[7].getBytes()) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.bytesToTuple(flds[8].getBytes()) : null);
            t.append(flds[9].compareTo("")!=0 ? ps.bytesToCharArray(flds[9].getBytes()) : null);
            
            assertTrue(inputTuple.equals(t));
            ++size;
        }
        FileLocalizer.delete(fSpec.getFileName(), pc);
    }

}
