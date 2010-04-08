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
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.Before;
import org.junit.Test;

public class TestPigStorage  {
        
    protected final Log log = LogFactory.getLog(getClass());
    
    private static MiniCluster cluster = MiniCluster.buildCluster();
    
    @Before
    public void setup() {
        // some tests are in map-reduce mode and some in local - so before
        // each test, we will de-initialize FileLocalizer so that temp files
        // are created correctly depending on the ExecType in the test.
        FileLocalizer.setInitialized(false);
    }
    
    @Test
    public void testBlockBoundary() throws ExecException {
        
        // This tests PigStorage loader with records exectly 
        // on the boundary of the file blocks.
        Properties props = new Properties();
        for (Entry<Object, Object> entry : cluster.getProperties().entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        props.setProperty("mapred.max.split.size", "20");
        PigServer pigServer = new PigServer(MAPREDUCE, props);
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
            
            pigServer.registerQuery("a = load '" + INPUT_FILE + "';");
            
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
    
    /**
     * Test to verify that PigStorage works fine in the following scenario:
     * The column prune optimization determines only columns 2 and 3 are needed
     * and there are records in the data which have only 1 column (malformed data).
     * In this case, PigStorage should return an empty tuple to represent columns
     * 2 and 3 and {@link POProject} would handle catching any 
     * {@link IndexOutOfBoundsException} resulting from accessing a field in the
     * tuple and substitute a null. 
     */
    @Test
    public void testPruneColumnsWithMissingFields() throws IOException {
        String inputFileName = "TestPigStorage-testPruneColumnsWithMissingFields-input.txt";
        Util.createLocalInputFile(
                inputFileName, 
                new String[] {"1\t2\t3", "4", "5\t6\t7"});
        PigServer ps = new PigServer(ExecType.LOCAL);
        String script = "a = load '" + inputFileName + "' as (i:int, j:int, k:int);" +
        		"b = foreach a generate j, k;";
        Util.registerMultiLineQuery(ps, script);
        Iterator<Tuple> it = ps.openIterator("b");
        assertEquals(Util.createTuple(new Integer[] { 2, 3}), it.next());
        assertEquals(Util.createTuple(new Integer[] { null, null}), it.next());
        assertEquals(Util.createTuple(new Integer[] { 6, 7}), it.next());
        assertFalse(it.hasNext());
                
    }

}
