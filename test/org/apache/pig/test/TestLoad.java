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
import java.io.FileReader;

import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLoad {
    FileSpec inpFSpec;
    POLoad ld;
    PigContext pc;
    DataBag inpDB;
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    @Before
    public void setUp() throws Exception {
        
        inpFSpec = new FileSpec("file:////etc/passwd",PigStorage.class.getName()+"(':')");
        pc = new PigContext();
        pc.connect();
        
        ld = GenPhyOp.topLoadOp();
        ld.setLFile(inpFSpec);
        ld.setPc(pc);
        
        inpDB = DefaultBagFactory.getInstance().newDefaultBag();
        BufferedReader br = new BufferedReader(new FileReader("/etc/passwd"));
        
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

}
