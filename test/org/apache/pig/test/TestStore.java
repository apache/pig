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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStore {
    POStore st;
    FileSpec fSpec;
    DataBag inpDB;
    static MiniCluster cluster = MiniCluster.buildCluster();
    PigContext pc;
    
    @Before
    public void setUp() throws Exception {
        st = GenPhyOp.topStoreOp();
        fSpec = new FileSpec("file:////tmp/storeTest.txt",PigStorage.class.getName()+"(':')");
        st.setSFile(fSpec);
        pc = new PigContext();
        pc.connect();
        st.setPc(pc);
        
        POProject proj = GenPhyOp.exprProject();
        proj.setColumn(0);
        proj.setResultType(DataType.TUPLE);
        proj.setOverloaded(true);
        List<PhysicalOperator> inps = new ArrayList<PhysicalOperator>();
        inps.add(proj);
        st.setInputs(inps);
        
        inpDB = GenRandomData.genRandSmallTupDataBag(new Random(), 10, 100);
        Tuple t = new DefaultTuple();
        t.append(inpDB);
        proj.attachInput(t);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testStore() throws ExecException, IOException {
        Result res = st.store();
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);
        
        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/storeTest.txt"));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split(":",-1);
            Tuple t = new DefaultTuple();
            t.append(flds[0].compareTo("")!=0 ? flds[0] : null);
            t.append(Integer.parseInt(flds[1]));
            
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
        FileLocalizer.delete(fSpec.getFileName(), pc);
    }

}
