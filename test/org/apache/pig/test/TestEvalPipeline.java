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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.junit.Before;
import org.junit.Test;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.TextLoader;
import org.apache.pig.data.*;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

import junit.framework.TestCase;

public class TestEvalPipeline extends TestCase {
    
    MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    static public class MyBagFunction extends EvalFunc<DataBag>{
        @Override
        public DataBag exec(Tuple input) throws IOException {
            TupleFactory tf = TupleFactory.getInstance();
            DataBag output = BagFactory.getInstance().newDefaultBag();
            output.add(tf.newTuple("a"));
            output.add(tf.newTuple("a"));
            output.add(tf.newTuple("a"));
            return output;
            
        }
    }
    
    
    private File createFile(String[] data) throws Exception{
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        for (int i=0; i<data.length; i++){
            pw.println(data[i]);
        }
        pw.close();
        return f;
    }
    
    @Test
    public void testFunctionInsideFunction() throws Exception{
        
        File f1 = createFile(new String[]{"a:1","b:1","a:1"});

        pigServer.registerQuery("a = load 'file:" + f1 + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate 1-1/1;");
        Iterator<Tuple> iter  = pigServer.openIterator("b");
        
        for (int i=0 ;i<3; i++){
            assertEquals(DataType.toDouble(iter.next().get(0)), 0.0);
        }
        
    }
    
    @Test
    public void testJoin() throws Exception{
                
        File f1 = createFile(new String[]{"a:1","b:1","a:1"});
        File f2 = createFile(new String[]{"b","b","a"});
        
        pigServer.registerQuery("a = load 'file:" + f1 + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = load 'file:" + f2 + "';");
        pigServer.registerQuery("c = cogroup a by $0, b by $0;");        
        pigServer.registerQuery("d = foreach c generate flatten($1),flatten($2);");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        int count = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(t.get(0).toString().equals(t.get(2).toString()));
            count++;
        }
        assertEquals(count, 4);
    }
    
    @Test
    public void testDriverMethod() throws Exception{
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        pw.println("a");
        pw.println("a");
        pw.close();
        pigServer.registerQuery("a = foreach (load 'file:" + f + "') generate 1, flatten(" + MyBagFunction.class.getName() + "(*));");
//        pigServer.registerQuery("b = foreach a generate $0, flatten($1);");
        Iterator<Tuple> iter = pigServer.openIterator("a");
        int count = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(t.get(0).toString().equals("1"));
            assertTrue(t.get(1).toString().equals("a"));
            count++;
        }
        assertEquals(count, 6);
        f.delete();
    }
    
    
    @Test
    public void testMapLookup() throws Exception {
        DataBag b = BagFactory.getInstance().newDefaultBag();
        Map<Object, Object> colors = new HashMap<Object, Object>();
        colors.put("apple","red");
        colors.put("orange","orange");
        
        Map<Object, Object> weights = new HashMap<Object, Object>();
        weights.put("apple","0.1");
        weights.put("orange","0.3");
        
        Tuple t = mTf.newTuple();
        t.append(colors);
        t.append(weights);
        b.add(t);
        
        String fileName = "file:"+File.createTempFile("tmp", "");
        PigFile f = new PigFile(fileName);
        f.store(b, new BinStorage(), pigServer.getPigContext());
        
        
        pigServer.registerQuery("a = load '" + fileName + "' using BinStorage();");
        pigServer.registerQuery("b = foreach a generate $0#'apple',flatten($1#'orange');");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        t = iter.next();
        assertEquals(t.get(0).toString(), "red");
        assertEquals(DataType.toDouble(t.get(1)), 0.3);
        assertFalse(iter.hasNext());
    }
    
    static public class TitleNGrams extends EvalFunc<DataBag> {
        
        @Override
        public DataBag exec(Tuple input) throws IOException {    
            try {
                DataBag output = BagFactory.getInstance().newDefaultBag();
                String str = input.get(0).toString();
            
                String title = str;

                if (title != null) {
                    List<String> nGrams = makeNGrams(title);
                    
                    for (Iterator<String> it = nGrams.iterator(); it.hasNext(); ) {
                        Tuple t = TupleFactory.getInstance().newTuple(1);
                        t.set(0, it.next());
                        output.add(t);
                    }
                }
    
                return output;
            } catch (ExecException ee) {
                IOException ioe = new IOException(ee.getMessage());
                ioe.initCause(ee);
                throw ioe;
            }
        }
        
        
        List<String> makeNGrams(String str) {
            List<String> tokens = new ArrayList<String>();
            
            StringTokenizer st = new StringTokenizer(str);
            while (st.hasMoreTokens())
                tokens.add(st.nextToken());
            
            return nGramHelper(tokens, new ArrayList<String>());
        }
        
        ArrayList<String> nGramHelper(List<String> str, ArrayList<String> nGrams) {
            if (str.size() == 0)
                return nGrams;
            
            for (int i = 0; i < str.size(); i++)
                nGrams.add(makeString(str.subList(0, i+1)));
            
            return nGramHelper(str.subList(1, str.size()), nGrams);
        }
        
        String makeString(List<String> list) {
            StringBuffer sb = new StringBuffer();
            for (Iterator<String> it = list.iterator(); it.hasNext(); ) {
                sb.append(it.next());
                if (it.hasNext())
                    sb.append(" ");
            }
            return sb.toString();
        }

        public Schema outputSchema(Schema input) {
            try {
            Schema stringSchema = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
            Schema.FieldSchema fs = new Schema.FieldSchema(null, stringSchema, DataType.BAG);
            return new Schema(fs);
            } catch (Exception e) {
                return null;
            }
        }
    }

    
    static public class Identity extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
           return input; 
        }

        public Schema outputSchema(Schema input) {
            return input;
        }
    }
    
    
    @Test
    public void testBagFunctionWithFlattening() throws Exception{
        File queryLogFile = createFile(
                    new String[]{ 
                        "stanford\tdeer\tsighting",
                        "bush\tpresident",
                        "stanford\tbush",
                        "conference\tyahoo",
                        "world\tcup\tcricket",
                        "bush\twins",
                        "stanford\tpresident",
                    }
                );
                
        File newsFile = createFile(
                    new String[]{
                        "deer seen at stanford",
                        "george bush visits stanford", 
                        "yahoo hosting a conference in the bay area", 
                        "who will win the world cup"
                    }
                );    
        
        Map<String, Integer> expectedResults = new HashMap<String, Integer>();
        expectedResults.put("bush", 2);
        expectedResults.put("stanford", 3);
        expectedResults.put("world", 1);
        expectedResults.put("conference", 1);
        
        pigServer.registerQuery("newsArticles = LOAD 'file:" + newsFile + "' USING " + TextLoader.class.getName() + "();");
        pigServer.registerQuery("queryLog = LOAD 'file:" + queryLogFile + "';");

        pigServer.registerQuery("titleNGrams = FOREACH newsArticles GENERATE flatten(" + TitleNGrams.class.getName() + "(*));");
        pigServer.registerQuery("cogrouped = COGROUP titleNGrams BY $0 INNER, queryLog BY $0 INNER;");
        pigServer.registerQuery("answer = FOREACH cogrouped GENERATE COUNT(queryLog),group;");
        
        Iterator<Tuple> iter = pigServer.openIterator("answer");
        if(!iter.hasNext()) fail("No Output received");
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(expectedResults.get(t.get(1).toString()).doubleValue(),(DataType.toDouble(t.get(0))).doubleValue());
        }
    }
    

    
    @Test
    public void testSort() throws Exception{
        testSortDistinct(false);
    }
    

    @Test
    public void testDistinct() throws Exception{
        testSortDistinct(true);
    }

    private void testSortDistinct(boolean eliminateDuplicates) throws Exception{
        int LOOP_SIZE = 1024*16;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_SIZE; i++) {
            ps.println(r.nextInt(LOOP_SIZE/2) + "\t" + i);
        }
        ps.close(); 
        
        String tmpOutputFile = FileLocalizer.getTemporaryPath(null, pigServer.getPigContext()).toString();
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        if (eliminateDuplicates){
            pigServer.registerQuery("B = DISTINCT (FOREACH A GENERATE $0) PARALLEL 10;");
        }else{
            pigServer.registerQuery("B = ORDER A BY $0 PARALLEL 10;");
        }
        pigServer.store("B", tmpOutputFile);
        
        pigServer.registerQuery("A = load '" + tmpOutputFile + "';");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        String last = "";
        HashSet<Integer> seen = new HashSet<Integer>();
        if(!iter.hasNext()) fail("No Results obtained");
        while (iter.hasNext()){
            Tuple t = iter.next();
            System.out.println(t.get(0).toString());
            if (eliminateDuplicates){
                Integer act = Integer.parseInt(t.get(0).toString());
                assertFalse(seen.contains(act));
                seen.add(act);
            }else{
                assertTrue(last.compareTo(t.get(0).toString())<=0);
                assertEquals(t.size(), 2);
                last = t.get(0).toString();
            }
        }
        
    }
    
    public void testNestedPlan() throws Exception{
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

        String tmpOutputFile = FileLocalizer.getTemporaryPath(null, 
        pigServer.getPigContext()).toString();
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "C1 = filter A by $0 > -1;"
        + "C2 = distinct C1;"
        + "C3 = distinct A;"
        + "generate (int)group," + Identity.class.getName() +"(*), COUNT(C2), SUM(C2.$1)," +  TitleNGrams.class.getName() + "(C3), MAX(C3.$1);"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals((Integer)numIdentity, (Integer)t.get(0));
            assertEquals((Long)5L, (Long)t.get(3));
            assertEquals(LOOP_COUNT*2.0, (Double)t.get(4), 0.01);
            assertEquals(8.0, (Double)t.get(6), 0.01);
            assertEquals(7, t.size());
            ++numIdentity;
        }
        assertEquals(LOOP_COUNT, numIdentity);
    }

    public void testLimit() throws Exception{
        int LOOP_COUNT = 20;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();

        String tmpOutputFile = FileLocalizer.getTemporaryPath(null, 
        pigServer.getPigContext()).toString();
        pigServer.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pigServer.registerQuery("B = limit A 5;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            ++numIdentity;
        }
        assertEquals(5, numIdentity);
    }


}
