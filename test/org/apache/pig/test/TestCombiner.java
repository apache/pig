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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

import org.apache.pig.impl.io.FileLocalizer;

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
        FileLocalizer.deleteTempFiles();
        runTest(new PigServer(ExecType.LOCAL, new Properties()));
        FileLocalizer.deleteTempFiles();
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
        Util.createInputFile(cluster, "MultiCombinerUseInput.txt", input);
        Properties props = cluster.getProperties();
        props.setProperty("io.sort.mb", "1");
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
        pigServer.registerQuery("a = load 'MultiCombinerUseInput.txt' as (x:int);");
        pigServer.registerQuery("b = group a all;");
        pigServer.registerQuery("c = foreach b generate COUNT(a), SUM(a.$0), MIN(a.$0), MAX(a.$0), AVG(a.$0);");

        // make sure there is a combine plan in the explain output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("c", ps);
        assertTrue(baos.toString().matches("(?si).*combine plan.*"));

        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        assertEquals(512000L, t.get(0));
        assertEquals(256000L, t.get(1));
        assertEquals(0, t.get(2));
        assertEquals(1, t.get(3));
        assertEquals(0.5, t.get(4));
        assertFalse(it.hasNext());
        Util.deleteFile(cluster, "MultiCombinerUseInput.txt");
    }
    
    @Test
    public void testDistinctAggs1() throws Exception {
        // test the use of combiner for distinct aggs:
        String input[] = {
                "pig1\t18\t2.1",
                "pig2\t24\t3.3",
                "pig5\t45\t2.4",
                "pig1\t18\t2.1",
                "pig1\t19\t2.1",
                "pig2\t24\t4.5",
                "pig1\t20\t3.1" };

        Util.createInputFile(cluster, "distinctAggs1Input.txt", input);
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load 'distinctAggs1Input.txt' as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("b = group a by name;");
        pigServer.registerQuery("c = foreach b  {" +
        		"        x = distinct a.age;" +
        		"        y = distinct a.gpa;" +
        		"        z = distinct a;" +
        		"        generate group, COUNT(x), SUM(x.age), SUM(y.gpa), SUM(a.age), " +
        		"                       SUM(a.gpa), COUNT(z.age), COUNT(z), SUM(z.age);};");
        
        // make sure there is a combine plan in the explain output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("c", ps);
        assertTrue(baos.toString().matches("(?si).*combine plan.*"));

        HashMap<String, Object[]> results = new HashMap<String, Object[]>();
        results.put("pig1", new Object[] {"pig1",3L,57L,5.2,75L,9.4,3L,3L,57L});
        results.put("pig2", new Object[] {"pig2",1L,24L,7.8,48L,7.8,2L,2L,48L});
        results.put("pig5", new Object[] {"pig5",1L,45L,2.4,45L,2.4,1L,1L,45L});
        Iterator<Tuple> it = pigServer.openIterator("c");
        while(it.hasNext()) {
            Tuple t = it.next();
            List<Object> fields = t.getAll();
            Object[] expected = results.get((String)fields.get(0));
            int i = 0;
            for (Object field : fields) {
                assertEquals(expected[i++], field);
            }
        }
        Util.deleteFile(cluster, "distinctAggs1Input.txt");
        
    }

    @Test
    public void testDistinctNoCombiner() throws Exception {
        // test that combiner is NOT invoked when
        // one of the elements in the foreach generate
        // is a distinct() as the leaf
        String input[] = {
                "pig1\t18\t2.1",
                "pig2\t24\t3.3",
                "pig5\t45\t2.4",
                "pig1\t18\t2.1",
                "pig1\t19\t2.1",
                "pig2\t24\t4.5",
                "pig1\t20\t3.1" };

        Util.createInputFile(cluster, "distinctNoCombinerInput.txt", input);
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load 'distinctNoCombinerInput.txt' as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("b = group a by name;");
        pigServer.registerQuery("c = foreach b  {" +
                "        z = distinct a;" +
                "        generate group, z, SUM(a.age), SUM(a.gpa);};");
        
        // make sure there is a combine plan in the explain output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("c", ps);
        assertFalse(baos.toString().matches("(?si).*combine plan.*"));

        HashMap<String, Object[]> results = new HashMap<String, Object[]>();
        results.put("pig1", new Object[] {"pig1","bag-place-holder",75L,9.4});
        results.put("pig2", new Object[] {"pig2","bag-place-holder",48L,7.8});
        results.put("pig5", new Object[] {"pig5","bag-place-holder",45L,2.4});
        Iterator<Tuple> it = pigServer.openIterator("c");
        while(it.hasNext()) {
            Tuple t = it.next();
            List<Object> fields = t.getAll();
            Object[] expected = results.get((String)fields.get(0));
            int i = 0;
            for (Object field : fields) {
                if(i == 1) {
                    // ignore the second field which is a bag
                    // for comparison here
                    continue;
                }
                assertEquals(expected[i++], field);
            }
        }
        Util.deleteFile(cluster, "distinctNoCombinerInput.txt");
        
    }

    @Test
    public void testForEachNoCombiner() throws Exception {
        // test that combiner is NOT invoked when
        // one of the elements in the foreach generate
        // has a foreach in the plan without a distinct agg
        String input[] = {
                "pig1\t18\t2.1",
                "pig2\t24\t3.3",
                "pig5\t45\t2.4",
                "pig1\t18\t2.1",
                "pig1\t19\t2.1",
                "pig2\t24\t4.5",
                "pig1\t20\t3.1" };

        Util.createInputFile(cluster, "forEachNoCombinerInput.txt", input);
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load 'forEachNoCombinerInput.txt' as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("b = group a by name;");
        pigServer.registerQuery("c = foreach b  {" +
                "        z = a.age;" +
                "        generate group, z, SUM(a.age), SUM(a.gpa);};");
        
        // make sure there is a combine plan in the explain output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("c", ps);
        assertFalse(baos.toString().matches("(?si).*combine plan.*"));

        HashMap<String, Object[]> results = new HashMap<String, Object[]>();
        results.put("pig1", new Object[] {"pig1","bag-place-holder",75L,9.4});
        results.put("pig2", new Object[] {"pig2","bag-place-holder",48L,7.8});
        results.put("pig5", new Object[] {"pig5","bag-place-holder",45L,2.4});
        Iterator<Tuple> it = pigServer.openIterator("c");
        while(it.hasNext()) {
            Tuple t = it.next();
            List<Object> fields = t.getAll();
            Object[] expected = results.get((String)fields.get(0));
            int i = 0;
            for (Object field : fields) {
                if(i == 1) {
                    // ignore the second field which is a bag
                    // for comparison here
                    continue;
                }
                assertEquals(expected[i++], field);
            }
        }
        Util.deleteFile(cluster, "forEachNoCombinerInput.txt");
        
    }

}
