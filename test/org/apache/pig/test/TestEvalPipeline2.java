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

import static org.apache.pig.ExecType.MAPREDUCE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.*;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.test.utils.Identity;

import junit.framework.TestCase;

public class TestEvalPipeline2 extends TestCase {
    
    MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
    }
    
    @Test
    public void testUDFwithStarInput() throws Exception {
        int LOOP_COUNT = 10;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "generate " + Identity.class.getName() +"(*);"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple tuple = iter.next();
            Tuple t = (Tuple)tuple.get(0);
            assertEquals(DataByteArray.class, t.get(0).getClass());
            int group = Integer.parseInt(new String(((DataByteArray)t.get(0)).get()));
            assertEquals(numIdentity, group);
            assertTrue(t.get(1) instanceof DataBag);
            DataBag bag = (DataBag)t.get(1);
            assertEquals(10, bag.size());
            assertEquals(2, t.size());
            ++numIdentity;
        }
        assertEquals(LOOP_COUNT, numIdentity);

    }

}
