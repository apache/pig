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


import java.util.Iterator;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFRJoinNullValue {

    private static MiniCluster cluster = MiniCluster.buildCluster();
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String[] input = new String[4];
        input[0] = 1 + "\t" + 2 + "\t" + 3;
        input[1] = "\t" + 2 + "\t" + 3;
        input[2] = "\t\t" + 3;
        Util.createInputFile(cluster, "input", input);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testNullMatch() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD 'input';");
        pigServer.registerQuery("B = LOAD 'input';");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbfrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));        
    }
    
    @Test
    public void testTupleNullMatch() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD 'input' as (x:int,y:int,z:int);");
        pigServer.registerQuery("B = LOAD 'input' as (x:int,y:int,z:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by (x, y), B by (x, y) using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by (x, y), B by (x, y);");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbfrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));        
    }
    
    @Test
    public void testLeftNullMatch() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD 'input' as (x:int,y:int, z:int);");
        pigServer.registerQuery("B = LOAD 'input' as (x:int,y:int, z:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbfrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));        
    }
    
    @Test
    public void testTupleLeftNullMatch() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD 'input' as (x:int,y:int,z:int);");
        pigServer.registerQuery("B = LOAD 'input' as (x:int,y:int,z:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by (x, y) left, B by (x, y) using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by (x, y) left, B by (x, y);");
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertEquals(dbfrj.size(), dbshj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));        
    }
}
