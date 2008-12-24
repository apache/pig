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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class TestCombiner extends TestCase {

    

    MiniCluster cluster = MiniCluster.buildCluster();
    
    @Test
    public void testOnCluster() throws Exception {
        // run the test on cluster        
        runTest(new PigServer(ExecType.MAPREDUCE, cluster.getProperties()));

    }

    @Test
    public void testLocal() throws Exception {
        // run the test locally
        runTest(new PigServer(ExecType.LOCAL, new Properties()));
    }

    
    private void runTest(PigServer pig) throws IOException {
        List<String> inputLines = new ArrayList<String>();
        inputLines.add("a,b,1");
        inputLines.add("a,b,1");
        inputLines.add("a,c,1");
        loadWithTestLoadFunc("A", pig, inputLines);

        pig.registerQuery("B = group A by ($0, $1);");
        pig.registerQuery("C = foreach B generate flatten(group), COUNT($1);");
        Iterator<Tuple> resultIterator = pig.openIterator("C");
        Tuple tuple = resultIterator.next();
        assertEquals("(a,b,2L)", tuple.toString());
        tuple = resultIterator.next();
        assertEquals("(a,c,1L)", tuple.toString());
    }

    private void loadWithTestLoadFunc(String loadAlias, PigServer pig,
            List<String> inputLines) throws IOException {
        File inputFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
        for (String line : inputLines) {
            ps.println(line);
        }
        ps.close();
        pig.registerQuery(loadAlias + " = load '"
                + Util.generateURI(inputFile.toString()) + "' using "
                + PigStorage.class.getName() + "(',');");
    }
    
    
    @Test
    public void testNoCombinerUse() {
        // To simulate this, we will have two input files
        // with exactly one input record - this should result
        // in two map tasks and each would process only one record
        // hence the combiner would not get called.
    }
    
    @Test
    public void testMultiCombinerUse() throws Exception {
        // test the scenario where the combiner is called multiple
        // times - this can happen when the output of the map > io.sort.mb
        // let's set the io.sort.mb to 1MB and > 1 MB map data.
        String[] input = new String[500*1024];
        for(int i = 0; i < input.length; i++) {
            if(i % 2 == 0) {
                input[i] = Integer.toString(1);
            } else {
                input[i] = Integer.toString(0);
            }
        }
        File f1 = Util.createFile(input);
        Properties props = cluster.getProperties();
        props.setProperty("io.sort.mb", "1");
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' as (x:int);");
        pigServer.registerQuery("b = group a all;");
        pigServer.registerQuery("c = foreach b generate COUNT(a), SUM(a.$0), MIN(a.$0), MAX(a.$0), AVG(a.$0);");
        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        assertEquals(512000L, t.get(0));
        assertEquals(256000L, t.get(1));
        assertEquals(0, t.get(2));
        assertEquals(1, t.get(3));
        assertEquals(0.5, t.get(4));
        assertFalse(it.hasNext());
    }

}
