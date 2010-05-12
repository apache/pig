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
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPigStorage {
        
    protected final Log log = LogFactory.getLog(getClass());
    
    private static MiniCluster cluster = MiniCluster.buildCluster();
    private static PigServer pigServer = null;
    
    
    @BeforeClass
    public static void setup() {
        try {
            pigServer = new PigServer(MAPREDUCE, cluster.getProperties());
        } catch (ExecException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
    
    @AfterClass
    public static void shutdown() {
        pigServer.shutdown();
        cluster.shutDown();
    }
    
    @Test
    public void testBlockBoundary() {
        
        // This tests PigStorage loader with records exectly 
        // on the boundary of the file blocks.
        String[] inputs = {
                "abcdefgh1", "abcdefgh2", "abcdefgh3", 
                "abcdefgh4", "abcdefgh5", "abcdefgh6",
                "abcdefgh7", "abcdefgh8", "abcdefgh9"
        };
        
        String[] expected = {
                "(abcdefgh1)", "(abcdefgh2)", "(abcdefgh3)", 
                "(abcdefgh4)", "(abcdefgh5)", "(abcdefgh6)",
                "(abcdefgh7)", "(abcdefgh8)", "(abcdefgh9)"
        };
        
        System.setProperty("pig.overrideBlockSize", "20");
        
        String INPUT_FILE = "tmp.txt";
        
        try {
                                    
            PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
            for (String s : inputs) {
                w.println(s);
            }
            w.close();
            
            Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
            
            pigServer.registerQuery("a = load 'file:" + INPUT_FILE + "';");
            
            Iterator<Tuple> iter = pigServer.openIterator("a");
            int counter = 0;
            while (iter.hasNext()){
                assertEquals(expected[counter++].toString(), iter.next().toString());
            }
            
            assertEquals(expected.length, counter);
        
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            new File(INPUT_FILE).delete();
            try {
                Util.deleteFile(cluster, INPUT_FILE);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    } 

}
