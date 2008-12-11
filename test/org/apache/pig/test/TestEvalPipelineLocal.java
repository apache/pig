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

import junit.framework.TestCase;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.TextLoader;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.utils.Identity;
import org.junit.Before;
import org.junit.Test;

public class TestEvalPipelineLocal extends TestCase {
    
    //MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
//        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer = new PigServer(ExecType.LOCAL);
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

        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
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
        
        pigServer.registerQuery("a = load '" + Util.generateURI(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = load '" + Util.generateURI(f2.toString()) + "';");
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
        pigServer.registerQuery("a = foreach (load '" + Util.generateURI(f.toString()) + "') generate 1, flatten(" + MyBagFunction.class.getName() + "(*));");
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
        
        
        pigServer.registerQuery("a = load '" + Util.encodeEscape(fileName) + "' using BinStorage();");
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

    static public class MapUDF extends EvalFunc<Map<Object, Object>> {
        @Override
        public Map<Object, Object> exec(Tuple input) throws IOException {

            TupleFactory tupleFactory = TupleFactory.getInstance();
            ArrayList<Object> objList = new ArrayList<Object>();
            objList.add(new Integer(1));
            objList.add(new Double(1.0));
            objList.add(new Float(1.0));
            objList.add(new String("World!"));
            Tuple tuple = tupleFactory.newTuple(objList);

            BagFactory bagFactory = BagFactory.getInstance();
            DataBag bag = bagFactory.newDefaultBag();
            bag.add(tuple);

            Map<Object, Object> mapInMap = new HashMap<Object, Object>();
            mapInMap.put("int", new Integer(10));
            mapInMap.put("float", new Float(10.0));

            Map<Object, Object> myMap = new HashMap<Object, Object>();
            myMap.put("string", new String("Hello"));
            myMap.put("int", new Integer(1));
            myMap.put("long", new Long(1));
            myMap.put("float", new Float(1.0));
            myMap.put("double", new Double(1.0));
            myMap.put("dba", new DataByteArray(new String("bytes").getBytes()));
            myMap.put("map", mapInMap);
            myMap.put("tuple", tuple);
            myMap.put("bag", bag);
            return myMap; 
        }

        public Schema outputSchema(Schema input) {
            return new Schema(new Schema.FieldSchema(null, DataType.MAP));
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
        
        pigServer.registerQuery("newsArticles = LOAD '" + Util.generateURI(newsFile.toString()) + "' USING " + TextLoader.class.getName() + "();");
        pigServer.registerQuery("queryLog = LOAD '" + Util.generateURI(queryLogFile.toString()) + "';");

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
    

    
    /*
    @Test
    public void testSort() throws Exception{
        testSortDistinct(false, false);
    }
    */
    
    @Test
    public void testSortWithUDF() throws Exception{
        testSortDistinct(false, true);
    }
    

    @Test
    public void testDistinct() throws Exception{
        testSortDistinct(true, false);
    }
    
    public static class TupComp extends ComparisonFunc {

        @Override
        public int compare(Tuple t1, Tuple t2) {
            return t1.compareTo(t2);
        }
    }

    private void testSortDistinct(boolean eliminateDuplicates, boolean useUDF) throws Exception{
        int LOOP_SIZE = 1024*16;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_SIZE; i++) {
            ps.println(r.nextInt(LOOP_SIZE/2) + "\t" + i);
        }
        ps.close(); 
        
        String tmpOutputFile = FileLocalizer.getTemporaryPath(null, pigServer.getPigContext()).toString();
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        if (eliminateDuplicates){
            pigServer.registerQuery("B = DISTINCT (FOREACH A GENERATE $0) PARALLEL 10;");
        }else{
            if(!useUDF) {
                pigServer.registerQuery("B = ORDER A BY $0 PARALLEL 10;");
            } else {
                pigServer.registerQuery("B = ORDER A BY $0 using " + TupComp.class.getName() + ";");
            }
        }
        pigServer.store("B", tmpOutputFile);
        
        pigServer.registerQuery("A = load '" + tmpOutputFile + "';");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        String last = "";
        HashSet<Integer> seen = new HashSet<Integer>();
        if(!iter.hasNext()) fail("No Results obtained");
        while (iter.hasNext()){
            Tuple t = iter.next();
            //System.out.println(t.get(0).toString());
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "C1 = filter A by $0 > -1;"
        + "C2 = distinct C1;"
        + "C3 = distinct A;"
        + "generate (int)group," + Identity.class.getName() +"(*), COUNT(C2), SUM(C2.$1)," +  TitleNGrams.class.getName() + "(C3), MAX(C3.$1), C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals((Integer)numIdentity, (Integer)t.get(0));
            assertEquals((Long)5L, (Long)t.get(2));
            assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            assertEquals(8.0, (Double)t.get(5), 0.01);
            assertEquals(5L, ((DataBag)t.get(6)).size());
            assertEquals(7, t.size());
            ++numIdentity;
        }
        assertEquals(LOOP_COUNT, numIdentity);
    }

    public void testNestedPlanWithExpressionAssignment() throws Exception{
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "C1 = filter A by $0 > -1;"
        + "C2 = distinct C1;"
        + "C3 = distinct A;"
        + "C4 = " + Identity.class.getName() + "(*);"
        + "C5 = COUNT(C2);"
        + "C6 = SUM(C2.$1);"
        + "C7 = " + TitleNGrams.class.getName() + "(C3);"
        + "C8 = MAX(C3.$1);"
        + "generate (int)group, C4, C5, C6, C7, C8, C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals((Integer)numIdentity, (Integer)t.get(0));
            assertEquals((Long)5L, (Long)t.get(2));
            assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            assertEquals(8.0, (Double)t.get(5), 0.01);
            assertEquals(5L, ((DataBag)t.get(6)).size());
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
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
    
    @Test
    public void testComplexData() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"{(f1, f2),(f3, f4)}\t(1,2)\t[key1#value1,key2#value2]"});
        
        pigServer.registerQuery("a = load '" + Util.generateURI(input.toString()) + "' using PigStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        pigServer.registerQuery("b = foreach a generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        assertEquals(new Long(2), t.get(0));
        assertEquals("1", t.get(1).toString());
        assertEquals("2", t.get(2).toString());
        assertEquals("value1", t.get(3).toString());
        assertEquals("value2", t.get(4).toString());
        
        //test with BinStorage
        pigServer.registerQuery("a = load '" + Util.generateURI(input.toString()) + "' using PigStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        String output = "./TestEvalPipeline-testComplexData";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        pigServer.registerQuery("x = load '" + output +"' using BinStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        pigServer.registerQuery("y = foreach x generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        it = pigServer.openIterator("y");
        t = it.next();
        assertEquals(new Long(2), t.get(0));
        assertEquals("1", t.get(1).toString());
        assertEquals("2", t.get(2).toString());
        assertEquals("value1", t.get(3).toString());
        assertEquals("value2", t.get(4).toString());
        pigServer.deleteFile(output);
        
    }
    
    
    @Test
    public void testBinStorageDetermineSchema() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"{(f1, f2),(f3, f4)}\t(1,2)\t[key1#value1,key2#value2]"});
        
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
                "as (b:bag{t:tuple(x:chararray,y:chararray)}, t2:tuple(a:int,b:int), m:map[]);");
        pigServer.registerQuery("b = foreach a generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        assertEquals(new Long(2), t.get(0));
        assertEquals(1, t.get(1));
        assertEquals(2, t.get(2));
        assertEquals("value1", t.get(3).toString());
        assertEquals("value2", t.get(4).toString());
        
        //test with BinStorage
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
                "as (b:bag{t:tuple(x:chararray,y:chararray)}, t2:tuple(a:int,b:int), m:map[]);");
        String output = "./TestEvalPipeline-testBinStorageDetermineSchema";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        // test with different load specifications
        String[] loads = {"p = load '" + output +"' using BinStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);",
                "p = load '" + output +"' using BinStorage() " +
                "as (b, t2, m);",
                "p = load '" + output +"' using BinStorage() ;"};
        // the corresponding generate statements
        String[] generates = {"q = foreach p generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2', b;",
                "q = foreach p generate COUNT(b), t2.$0, t2.$1, m#'key1', m#'key2', b;",
                "q = foreach p generate COUNT($0), $1.$0, $1.$1, $2#'key1', $2#'key2', $0;"};
        
        for (int i = 0; i < loads.length; i++) {
            pigServer.registerQuery(loads[i]);
            pigServer.registerQuery(generates[i]);
            it = pigServer.openIterator("q");
            t = it.next();
            assertEquals(new Long(2), t.get(0));
            assertEquals(Integer.class, t.get(1).getClass());
            assertEquals(1, t.get(1));
            assertEquals(Integer.class, t.get(2).getClass());
            assertEquals(2, t.get(2));
            assertEquals("value1", t.get(3).toString());
            assertEquals("value2", t.get(4).toString());
            assertEquals(DefaultDataBag.class, t.get(5).getClass());
            DataBag bg = (DataBag)t.get(5);
            for (Iterator<Tuple> bit = bg.iterator(); bit.hasNext();) {
                Tuple bt = bit.next();
                assertEquals(String.class, bt.get(0).getClass());
                assertEquals(String.class, bt.get(1).getClass());            
            }
        }        
        pigServer.deleteFile(output);
    }

    @Test
    public void testProjectBag() throws IOException, ExecException {
        // This tests make sure that when a bag with multiple columns is
        // projected all columns apear in the output
        File input = Util.createInputFile("tmp", "", 
                new String[] {"f1\tf2\tf3"});
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' as (x, y, z);");
        pigServer.registerQuery("b = group a by x;");
        pigServer.registerQuery("c = foreach b generate flatten(a.(y, z));");
        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        assertEquals(2, t.size());
        assertEquals("f2", t.get(0).toString());
        assertEquals("f3", t.get(1).toString());
    }

    @Test
    public void testBinStorageDetermineSchema2() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"pigtester\t10\t1.2"});
        
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
                "as (name:chararray, age:int, gpa:double);");
        String output = "./TestEvalPipeline-testBinStorageDetermineSchema2";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        // test with different load specifications
        String[] loads = {"p = load '" + output +"' using BinStorage() " +
                "as (name:chararray, age:int, gpa:double);",
                "p = load '" + output +"' using BinStorage() " +
                "as (name, age, gpa);",
                "p = load '" + output +"' using BinStorage() ;"};
        // the corresponding generate statements
        String[] generates = {"q = foreach p generate name, age, gpa;",
                "q = foreach p generate name, age, gpa;",
                "q = foreach p generate $0, $1, $2;"};
        
        for (int i = 0; i < loads.length; i++) {
            pigServer.registerQuery(loads[i]);
            pigServer.registerQuery(generates[i]);
            Iterator<Tuple> it = pigServer.openIterator("q");
            Tuple t = it.next();
            assertEquals("pigtester", t.get(0));
            assertEquals(String.class, t.get(0).getClass());
            assertEquals(10, t.get(1));
            assertEquals(Integer.class, t.get(1).getClass());
            assertEquals(1.2, t.get(2));
            assertEquals(Double.class, t.get(2).getClass());
        }
        
        // test that valid casting is allowed
        pigServer.registerQuery("p = load '" + output + "' using BinStorage() " +
                " as (name, age:long, gpa:float);");
        pigServer.registerQuery("q = foreach p generate name, age, gpa;");
        Iterator<Tuple> it = pigServer.openIterator("q");
        Tuple t = it.next();
        assertEquals("pigtester", t.get(0));
        assertEquals(String.class, t.get(0).getClass());
        assertEquals(10L, t.get(1));
        assertEquals(Long.class, t.get(1).getClass());
        assertEquals(1.2f, t.get(2));
        assertEquals(Float.class, t.get(2).getClass());
        
        // test that implicit casts work
        pigServer.registerQuery("p = load '" + output + "' using BinStorage() " +
        " as (name, age, gpa);");
        pigServer.registerQuery("q = foreach p generate name, age + 1L, (int)gpa;");
        it = pigServer.openIterator("q");
        t = it.next();
        assertEquals("pigtester", t.get(0));
        assertEquals(String.class, t.get(0).getClass());
        assertEquals(11L, t.get(1));
        assertEquals(Long.class, t.get(1).getClass());
        assertEquals(1, t.get(2));
        assertEquals(Integer.class, t.get(2).getClass());
        pigServer.deleteFile(output);
    }
    
    @Test
    public void testCogroupWithInputFromGroup() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"pigtester\t10\t1.2", "pigtester\t15\t1.2", 
                "pigtester2\t10\t1.2",
                "pigtester3\t10\t1.2", "pigtester3\t20\t1.2", "pigtester3\t30\t1.2"});
        
        Map<String, Pair<Long, Long>> resultMap = new HashMap<String, Pair<Long, Long>>();
        // we will in essence be doing a group on first column and getting
        // SUM over second column and a count for the group - store
        // the results for the three groups above so we can check the output
        resultMap.put("pigtester", new Pair<Long, Long>(25L, 2L));
        resultMap.put("pigtester2", new Pair<Long, Long>(10L, 1L));
        resultMap.put("pigtester3", new Pair<Long, Long>(60L, 3L));
        
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
                "as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("b = group a by name;");
        pigServer.registerQuery("c = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
        "as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("d = cogroup b by group, c by name;");
        pigServer.registerQuery("e = foreach d generate flatten(group), SUM(c.age), COUNT(c.name);");
        Iterator<Tuple> it = pigServer.openIterator("e");
        for(int i = 0; i < resultMap.size(); i++) {
            Tuple t = it.next();
            assertEquals(true, resultMap.containsKey(t.get(0)));
            Pair<Long, Long> output = resultMap.get(t.get(0)); 
            assertEquals(output.first, t.get(1));
            assertEquals(output.second, t.get(2));
        }
    }
    
    @Test
    public void testUtf8Dump() throws IOException, ExecException {
        
        // Create input file with unicode data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"wendyξ"});
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + "' using PigStorage() " +
        "as (name:chararray);");
        Iterator<Tuple> it = pigServer.openIterator("a");
        Tuple t = it.next();
        assertEquals("wendyξ", t.get(0));
        
    }

    public void testMapUDF() throws Exception{
        int LOOP_COUNT = 2;
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = foreach A generate " + MapUDF.class.getName() + "($0) as mymap;"); //the argument does not matter
        String query = "C = foreach B {"
        + "generate (double)mymap#'double' as d, (long)mymap#'long' + (float)mymap#'float' as float_sum, CONCAT((chararray) mymap#'string', ' World!'), mymap#'int' * 10, (bag{tuple()}) mymap#'bag' as mybag, (tuple()) mymap#'tuple' as mytuple, (map[])mymap#'map' as mapInMap, mymap#'dba' as dba;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(1.0, (Double)t.get(0), 0.01);
            assertEquals(2.0, (Float)t.get(1), 0.01);
            assertTrue(((String)t.get(2)).equals("Hello World!"));
            assertEquals(new Integer(10), (Integer)t.get(3));
            assertEquals(1, ((DataBag)t.get(4)).size());
            assertEquals(4, ((Tuple)t.get(5)).size());
            assertEquals(2, ((Map<Object, Object>)t.get(6)).size());
            assertEquals(DataByteArray.class, t.get(7).getClass());
            assertEquals(8, t.size());
            ++numIdentity;
        }
        assertEquals(LOOP_COUNT * LOOP_COUNT, numIdentity);
    }

    public void testMapUDFFail() throws Exception{
        int LOOP_COUNT = 2;
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = foreach A generate " + MapUDF.class.getName() + "($0) as mymap;"); //the argument does not matter
        String query = "C = foreach B {"
        + "generate mymap#'dba' * 10;"
        + "};";

        pigServer.registerQuery(query);
        try {
            Iterator<Tuple> iter = pigServer.openIterator("C");
            fail("Error expected.");
        } catch (Exception e) {
            e.getMessage().contains("Cannot determine");
        }
    }

    @Test
    public void testLoadCtorArgs() throws IOException, ExecException {
        
        // Create input file
        File input = Util.createInputFile("tmp", "", 
                new String[] {"hello:world"});
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(input.toString()) + 
                "' using org.apache.pig.test.PigStorageNoDefCtor(':');");
        pigServer.registerQuery("b = foreach a generate (chararray)$0, (chararray)$1;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        assertEquals("hello", t.get(0));
        assertEquals("world", t.get(1));
        
    }

    @Test
    public void testNestedPlanForCloning() throws Exception{
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "C1 = filter A by not($0 <= -1);"
        + "C2 = distinct C1;"
        + "C3 = distinct A;"
        + "C4 = order A by $0;"
        + "generate (group + 1) * 10, COUNT(C4), COUNT(C2), SUM(C2.$1)," +  TitleNGrams.class.getName() + "(C3), MAX(C3.$1), C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals((Integer)((numIdentity + 1) * 10), (Integer)t.get(0));
            assertEquals((Long)10L, (Long)t.get(1));
            assertEquals((Long)5L, (Long)t.get(2));
            assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            assertEquals(8.0, (Double)t.get(5), 0.01);
            assertEquals(5L, ((DataBag)t.get(6)).size());
            assertEquals(7, t.size());
            ++numIdentity;
        }
        assertEquals(LOOP_COUNT, numIdentity);
    }

    @Test
    public void testArithmeticCloning() throws Exception{
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = distinct A;");
        String query = "C = foreach B {"
        + "C1 = $1 - $0;"
        + "C2 = $1%2;"
        + "C3 = ($1 == 0? 0 : $0/$1);"
        + "generate C1, C2, C3;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");

        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                assertEquals(3, t.size());
                assertEquals(new Double(j - i), (Double)t.get(0), 0.01);
                assertEquals((Integer)(j%2), (Integer)t.get(1));
                if(j == 0) {
                    assertEquals(0.0, (Double)t.get(2), 0.01);
                } else {
                    assertEquals((Double)((double)i/j), (Double)t.get(2), 0.01);
                }
                ++numRows;
            }
        }

        assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }

    @Test
    public void testExpressionReUse() throws Exception{
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = distinct A;");
        String query = "C = foreach B {"
        + "C1 = $0 + $1;"
        + "C2 = C1 + $0;"
        + "generate C1, C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");

        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                assertEquals(2, t.size());
                assertEquals(new Double(i + j), (Double)t.get(0), 0.01);
                assertEquals(new Double(i + j + i), (Double)t.get(1));
                ++numRows;
            }
        }

        assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }

    @Test
    public void testIdentity() throws Exception{
        int LOOP_COUNT = 2;
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
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = distinct A ;"); //the argument does not matter
        pigServer.registerQuery("C = foreach B generate FLATTEN(" + Identity.class.getName() + "($0, $1));"); //the argument does not matter

        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) fail("No output found");
        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                assertEquals(2, t.size());
                assertEquals(new Double(i), new Double(t.get(0).toString()), 0.01);
                assertEquals(new Double(j), new Double(t.get(1).toString()), 0.01);
                ++numRows;
            }
        }

        assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }

}
