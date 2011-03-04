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

import junit.framework.Assert;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
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
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.Identity;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestEvalPipeline {
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;
    private PigContext pigContext;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    
    @Before
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
        pigContext = pigServer.getPigContext();
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
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
    
    @Test
    public void testFunctionInsideFunction() throws Exception{
        File f1 = Util.createFile(new String[]{"a:1","b:1","a:1"});

        pigServer.registerQuery("a = load '" 
                + Util.generateURI(f1.toString(), pigContext) 
                + "' using " + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = foreach a generate 1-1/1;");
        Iterator<Tuple> iter  = pigServer.openIterator("b");
        
        for (int i=0 ;i<3; i++){
            Assert.assertEquals(DataType.toDouble(iter.next().get(0)), 0.0);
        }
    }
    
    @Test
    public void testJoin() throws Exception{
        File f1 = Util.createFile(new String[]{"a:1","b:1","a:1"});
        File f2 = Util.createFile(new String[]{"b","b","a"});
        
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(f1.toString(), pigContext) + "' using " 
                + PigStorage.class.getName() + "(':');");
        pigServer.registerQuery("b = load '" 
                + Util.generateURI(f2.toString(), pigContext) + "';");
        pigServer.registerQuery("c = cogroup a by $0, b by $0;");        
        pigServer.registerQuery("d = foreach c generate flatten($1),flatten($2);");
        
        Iterator<Tuple> iter = pigServer.openIterator("d");
        int count = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertTrue(t.get(0).toString().equals(t.get(2).toString()));
            count++;
        }
        Assert.assertEquals(count, 4);
    }
    
    @Test
    public void testDriverMethod() throws Exception{
        File f = Util.createTempFileDelOnExit("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        pw.println("a");
        pw.println("a");
        pw.close();
        pigServer.registerQuery("a = foreach (load '" 
                + Util.generateURI(f.toString(), pigContext) + "') " 
                + "generate 1, flatten(" + MyBagFunction.class.getName() + "(*));");
        Iterator<Tuple> iter = pigServer.openIterator("a");
        int count = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertTrue(t.get(0).toString().equals("1"));
            Assert.assertTrue(t.get(1).toString().equals("a"));
            count++;
        }
        Assert.assertEquals(count, 6);
        f.delete();
    }
    
    @Test
    public void testMapLookup() throws Exception {
        DataBag b = BagFactory.getInstance().newDefaultBag();
        Map<String, Object> colors = new HashMap<String, Object>();
        colors.put("apple","red");
        colors.put("orange","orange");
        
        Map<String, Object> weights = new HashMap<String, Object>();
        weights.put("apple","0.1");
        weights.put("orange","0.3");
        
        Tuple t = mTf.newTuple();
        t.append(colors);
        t.append(weights);
        b.add(t);
        
        File tmpFile = Util.createTempFileDelOnExit("tmp", "");
        tmpFile.deleteOnExit();
        String fileName = tmpFile.getAbsolutePath();
        PigFile f = new PigFile(fileName);
        f.store(b, new FuncSpec(BinStorage.class.getCanonicalName()),
                pigServer.getPigContext());        
        
        pigServer.registerQuery("a = load '" + fileName + "' using BinStorage();");
        pigServer.registerQuery("b = foreach a generate $0#'apple',flatten($1#'orange');");
        Iterator<Tuple> iter = pigServer.openIterator("b");
        t = iter.next();
        Assert.assertEquals(t.get(0).toString(), "red");
        Assert.assertEquals(DataType.toDouble(t.get(1)), 0.3);
        Assert.assertFalse(iter.hasNext());
        Util.deleteFile(cluster, fileName);
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

    static public class MapUDF extends EvalFunc<Map<String, Object>> {
        @Override
        public Map<String, Object> exec(Tuple input) throws IOException {

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

            Map<String, Object> mapInMap = new HashMap<String, Object>();
            mapInMap.put("int", new Integer(10));
            mapInMap.put("float", new Float(10.0));

            Map<String, Object> myMap = new HashMap<String, Object>();
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
        File queryLogFile = Util.createFile(
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
                
        File newsFile = Util.createFile(
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
        
        pigServer.registerQuery("newsArticles = LOAD '" 
                + Util.generateURI(newsFile.toString(), pigContext) 
                + "' USING " + TextLoader.class.getName() + "();");
        pigServer.registerQuery("queryLog = LOAD '" 
                + Util.generateURI(queryLogFile.toString(), pigContext) + "';");

        pigServer.registerQuery("titleNGrams = FOREACH newsArticles GENERATE flatten(" + TitleNGrams.class.getName() + "(*));");
        pigServer.registerQuery("cogrouped = COGROUP titleNGrams BY $0 INNER, queryLog BY $0 INNER;");
        pigServer.registerQuery("answer = FOREACH cogrouped GENERATE COUNT(queryLog),group;");
        
        Iterator<Tuple> iter = pigServer.openIterator("answer");
        if(!iter.hasNext()) Assert.fail("No Output received");
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertEquals(expectedResults.get(t.get(1).toString()).doubleValue(),(DataType.toDouble(t.get(0))).doubleValue());
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
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_SIZE; i++) {
            ps.println(r.nextInt(LOOP_SIZE/2) + "\t" + i);
        }
        ps.close(); 
        
        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        if (eliminateDuplicates){
            pigServer.registerQuery("B = DISTINCT (FOREACH A GENERATE $0) PARALLEL 10;");
        }else{
            if(!useUDF) {
                pigServer.registerQuery("B = ORDER A BY $0 PARALLEL 10;");
            } else {
                pigServer.registerQuery("B = ORDER A BY $0 using " + TupComp.class.getName() + ";");
            }
        }
        Iterator<Tuple> iter = pigServer.openIterator("B");
        String last = "";
        HashSet<Integer> seen = new HashSet<Integer>();
        if(!iter.hasNext()) Assert.fail("No Results obtained");
        while (iter.hasNext()){
            Tuple t = iter.next();
            if (eliminateDuplicates){
                Integer act = Integer.parseInt(t.get(0).toString());
                Assert.assertFalse(seen.contains(act));
                seen.add(act);
            }else{
                Assert.assertTrue(last.compareTo(t.get(0).toString())<=0);
                Assert.assertEquals(t.size(), 2);
                last = t.get(0).toString();
            }
        }        
    }
    
    @Test
    public void testNestedPlan() throws Exception{
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = group A by $0;");
        String query = "C = foreach B {"
        + "C1 = filter A by $0 > -1;"
        + "C2 = distinct C1;"
        + "C3 = distinct A;"
        + "generate (int)group," + Identity.class.getName() +"(*), COUNT(C2), SUM(C2.$1)," +  TitleNGrams.class.getName() + "(C3), MAX(C3.$1), C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertEquals((Integer)numIdentity, (Integer)t.get(0));
            Assert.assertEquals((Long)5L, (Long)t.get(2));
            Assert.assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            Assert.assertEquals(8.0, (Double)t.get(5), 0.01);
            Assert.assertEquals(5L, ((DataBag)t.get(6)).size());
            Assert.assertEquals(7, t.size());
            ++numIdentity;
        }
        Assert.assertEquals(LOOP_COUNT, numIdentity);
    }

    @Test
    public void testNestedPlanWithExpressionAssignment() throws Exception{
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
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
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertEquals((Integer)numIdentity, (Integer)t.get(0));
            Assert.assertEquals((Long)5L, (Long)t.get(2));
            Assert.assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            Assert.assertEquals(8.0, (Double)t.get(5), 0.01);
            Assert.assertEquals(5L, ((DataBag)t.get(6)).size());
            Assert.assertEquals(7, t.size());
            ++numIdentity;
        }
        Assert.assertEquals(LOOP_COUNT, numIdentity);
    }

    @Test
    public void testLimit() throws Exception{
        int LOOP_COUNT = 20;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = limit A 5;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            iter.next();
            ++numIdentity;
        }
        Assert.assertEquals(5, numIdentity);
    }
    
    @Test
    public void testComplexData() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"{(f1, f2),(f3, f4)}\t(1,2)\t[key1#value1,key2#value2]"});
        
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        pigServer.registerQuery("b = foreach a generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Assert.assertEquals(new Long(2), t.get(0));
        Assert.assertEquals("1", t.get(1).toString());
        Assert.assertEquals("2", t.get(2).toString());
        Assert.assertEquals("value1", t.get(3).toString());
        Assert.assertEquals("value2", t.get(4).toString());
        
        //test with BinStorage
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        String output = "/pig/out/TestEvalPipeline-testComplexData";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        pigServer.registerQuery("x = load '" + output +"' using BinStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);");
        pigServer.registerQuery("y = foreach x generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        it = pigServer.openIterator("y");
        t = it.next();
        Assert.assertEquals(new Long(2), t.get(0));
        Assert.assertEquals("1", t.get(1).toString());
        Assert.assertEquals("2", t.get(2).toString());
        Assert.assertEquals("value1", t.get(3).toString());
        Assert.assertEquals("value2", t.get(4).toString());        
    }

    @Test
    public void testBinStorageDetermineSchema() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"{(f1, f2),(f3, f4)}\t(1,2)\t[key1#value1,key2#value2]"});
        
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (b:bag{t:tuple(x:chararray,y:chararray)}, t2:tuple(a:int,b:int), m:map[]);");
        pigServer.registerQuery("b = foreach a generate COUNT(b), t2.a, t2.b, m#'key1', m#'key2';");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Assert.assertEquals(new Long(2), t.get(0));
        Assert.assertEquals(1, t.get(1));
        Assert.assertEquals(2, t.get(2));
        Assert.assertEquals("value1", t.get(3).toString());
        Assert.assertEquals("value2", t.get(4).toString());
        
        //test with BinStorage
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (b:bag{t:tuple(x:chararray,y:chararray)}, t2:tuple(a:int,b:int), m:map[]);");
        String output = "/pig/out/TestEvalPipeline-testBinStorageDetermineSchema";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        // test with different load specifications
        String[] loads = {"p = load '" + output +"' using BinStorage() " +
                "as (b:bag{t:tuple(x,y)}, t2:tuple(a,b), m:map[]);",
                "p = load '" + output +"' using BinStorage() " +
                "as (b, t2, m);",
                "p = load '" + output +"' using BinStorage() ;"};
        // the corresponding generate statements
        String[] generates = {"q = foreach p generate COUNT(b), t2.a, t2.b as t2b, m#'key1', m#'key2', b;",
                "q = foreach p generate COUNT(b), t2.$0, t2.$1, m#'key1', m#'key2', b;",
                "q = foreach p generate COUNT($0), $1.$0, $1.$1, $2#'key1', $2#'key2', $0;"};
        
        for (int i = 0; i < loads.length; i++) {
            pigServer.registerQuery(loads[i]);
            pigServer.registerQuery(generates[i]);
            it = pigServer.openIterator("q");
            t = it.next();
            Assert.assertEquals(new Long(2), t.get(0));
            Assert.assertEquals(Integer.class, t.get(1).getClass());
            Assert.assertEquals(1, t.get(1));
            Assert.assertEquals(Integer.class, t.get(2).getClass());
            Assert.assertEquals(2, t.get(2));
            Assert.assertEquals("value1", t.get(3).toString());
            Assert.assertEquals("value2", t.get(4).toString());
            Assert.assertEquals(DefaultDataBag.class, t.get(5).getClass());
            DataBag bg = (DataBag)t.get(5);
            for (Iterator<Tuple> bit = bg.iterator(); bit.hasNext();) {
                Tuple bt = bit.next();
                Assert.assertEquals(String.class, bt.get(0).getClass());
                Assert.assertEquals(String.class, bt.get(1).getClass());            
            }
        }        
    }

    @Test
    public void testProjectBag() throws IOException, ExecException {
        // This tests make sure that when a bag with multiple columns is
        // projected all columns apear in the output
        File input = Util.createInputFile("tmp", "", 
                new String[] {"f1\tf2\tf3"});
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' as (x, y, z);");
        pigServer.registerQuery("b = group a by x;");
        pigServer.registerQuery("c = foreach b generate flatten(a.(y, z));");
        Iterator<Tuple> it = pigServer.openIterator("c");
        Tuple t = it.next();
        Assert.assertEquals(2, t.size());
        Assert.assertEquals("f2", t.get(0).toString());
        Assert.assertEquals("f3", t.get(1).toString());
    }

    @Test
    public void testBinStorageDetermineSchema2() throws IOException, ExecException {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"pigtester\t10\t1.2"});
        
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (name:chararray, age:int, gpa:double);");
        String output = "/pig/out/TestEvalPipeline-testBinStorageDetermineSchema2";
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
            Assert.assertEquals("pigtester", t.get(0));
            Assert.assertEquals(String.class, t.get(0).getClass());
            Assert.assertEquals(10, t.get(1));
            Assert.assertEquals(Integer.class, t.get(1).getClass());
            Assert.assertEquals(1.2, t.get(2));
            Assert.assertEquals(Double.class, t.get(2).getClass());
        }
        
        // test that valid casting is allowed
        pigServer.registerQuery("p = load '" + output + "' using BinStorage() " +
                " as (name, age:long, gpa:float);");
        pigServer.registerQuery("q = foreach p generate name, age, gpa;");
        Iterator<Tuple> it = pigServer.openIterator("q");
        Tuple t = it.next();
        Assert.assertEquals("pigtester", t.get(0));
        Assert.assertEquals(String.class, t.get(0).getClass());
        Assert.assertEquals(10L, t.get(1));
        Assert.assertEquals(Long.class, t.get(1).getClass());
        Assert.assertEquals(1.2f, t.get(2));
        Assert.assertEquals(Float.class, t.get(2).getClass());
        
        // test that implicit casts work
        pigServer.registerQuery("p = load '" + output + "' using BinStorage() " +
        " as (name, age, gpa);");
        pigServer.registerQuery("q = foreach p generate name, age + 1L, (int)gpa;");
        it = pigServer.openIterator("q");
        t = it.next();
        Assert.assertEquals("pigtester", t.get(0));
        Assert.assertEquals(String.class, t.get(0).getClass());
        Assert.assertEquals(11L, t.get(1));
        Assert.assertEquals(Long.class, t.get(1).getClass());
        Assert.assertEquals(1, t.get(2));
        Assert.assertEquals(Integer.class, t.get(2).getClass());
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
        
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("b = group a by name;");
        pigServer.registerQuery("c = load '" 
                + Util.generateURI(input.toString(), pigContext) + "' using PigStorage() " 
                + "as (name:chararray, age:int, gpa:double);");
        pigServer.registerQuery("d = cogroup b by group, c by name;");
        pigServer.registerQuery("e = foreach d generate flatten(group), SUM(c.age), COUNT(c.name);");
        Iterator<Tuple> it = pigServer.openIterator("e");
        for(int i = 0; i < resultMap.size(); i++) {
            Tuple t = it.next();
            Assert.assertEquals(true, resultMap.containsKey(t.get(0)));
            Pair<Long, Long> output = resultMap.get(t.get(0)); 
            Assert.assertEquals(output.first, t.get(1));
            Assert.assertEquals(output.second, t.get(2));
        }
    }
    
    @Test
    public void testUtf8Dump() throws IOException, ExecException {
        
        // Create input file with unicode data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"wendyξ"});
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) 
                + "' using PigStorage() " + "as (name:chararray);");
        Iterator<Tuple> it = pigServer.openIterator("a");
        Tuple t = it.next();
        Assert.assertEquals("wendyξ", t.get(0));
        
    }

    @Test
    public void testMapUDF() throws Exception{
        int LOOP_COUNT = 2;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = foreach A generate " + MapUDF.class.getName() + "($0) as mymap;"); //the argument does not matter
        String query = "C = foreach B {"
        + "generate (double)mymap#'double' as d, (long)mymap#'long' + (float)mymap#'float' as float_sum, CONCAT((chararray) mymap#'string', ' World!'), mymap#'int' * 10, (bag{tuple()}) mymap#'bag' as mybag, (tuple()) mymap#'tuple' as mytuple, (map[])mymap#'map' as mapInMap, mymap#'dba' as dba;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertEquals(1.0, (Double)t.get(0), 0.01);
            Assert.assertEquals(2.0, (Float)t.get(1), 0.01);
            Assert.assertTrue(((String)t.get(2)).equals("Hello World!"));
            Assert.assertEquals(new Integer(10), (Integer)t.get(3));
            Assert.assertEquals(1, ((DataBag)t.get(4)).size());
            Assert.assertEquals(4, ((Tuple)t.get(5)).size());
            Assert.assertEquals(2, ((Map<String, Object>)t.get(6)).size());
            Assert.assertEquals(DataByteArray.class, t.get(7).getClass());
            Assert.assertEquals(8, t.size());
            ++numIdentity;
        }
        Assert.assertEquals(LOOP_COUNT * LOOP_COUNT, numIdentity);
    }

    @Test
    public void testMapUDFfail() throws Exception{
        int LOOP_COUNT = 2;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = foreach A generate " + MapUDF.class.getName() + "($0) as mymap;"); //the argument does not matter
        String query = "C = foreach B {"
        + "generate mymap#'dba' * 10;"
        + "};";

        pigServer.registerQuery(query);
        try {
            pigServer.openIterator("C");
            Assert.fail("Error expected.");
        } catch (Exception e) {
            e.getMessage().contains("Cannot determine");
        }
    }

    @Test
    public void testLoadCtorArgs() throws IOException, ExecException {
        
        // Create input file
        File input = Util.createInputFile("tmp", "", 
                new String[] {"hello:world"});
        pigServer.registerQuery("a = load '" 
                + Util.generateURI(input.toString(), pigContext) 
                + "' using org.apache.pig.test.PigStorageNoDefCtor(':');");
        pigServer.registerQuery("b = foreach a generate (chararray)$0, (chararray)$1;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Assert.assertEquals("hello", t.get(0));
        Assert.assertEquals("world", t.get(1));
        
    }

    @Test
    public void testNestedPlanForCloning() throws Exception{
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
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
        if(!iter.hasNext()) Assert.fail("No output found");
        int numIdentity = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            Assert.assertEquals((Integer)((numIdentity + 1) * 10), (Integer)t.get(0));
            Assert.assertEquals((Long)10L, (Long)t.get(1));
            Assert.assertEquals((Long)5L, (Long)t.get(2));
            Assert.assertEquals(LOOP_COUNT*2.0, (Double)t.get(3), 0.01);
            Assert.assertEquals(8.0, (Double)t.get(5), 0.01);
            Assert.assertEquals(5L, ((DataBag)t.get(6)).size());
            Assert.assertEquals(7, t.size());
            ++numIdentity;
        }
        Assert.assertEquals(LOOP_COUNT, numIdentity);
    }

    @Test
    public void testArithmeticCloning() throws Exception{
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = distinct A;");
        String query = "C = foreach B {"
        + "C1 = $1 - $0;"
        + "C2 = $1%2;"
        + "C3 = ($1 == 0? 0 : $0/$1);"
        + "generate C1, C2, C3;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");

        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                Assert.assertEquals(3, t.size());
                Assert.assertEquals(new Double(j - i), (Double)t.get(0), 0.01);
                Assert.assertEquals((Integer)(j%2), (Integer)t.get(1));
                if(j == 0) {
                    Assert.assertEquals(0.0, (Double)t.get(2), 0.01);
                } else {
                    Assert.assertEquals((Double)((double)i/j), (Double)t.get(2), 0.01);
                }
                ++numRows;
            }
        }

        Assert.assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }

    @Test
    public void testExpressionReUse() throws Exception{
        int LOOP_COUNT = 10;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = distinct A;");
        String query = "C = foreach B {"
        + "C1 = $0 + $1;"
        + "C2 = C1 + $0;"
        + "generate C1, C2;"
        + "};";

        pigServer.registerQuery(query);
        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");

        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                Assert.assertEquals(2, t.size());
                Assert.assertEquals(new Double(i + j), (Double)t.get(0), 0.01);
                Assert.assertEquals(new Double(i + j + i), (Double)t.get(1));
                ++numRows;
            }
        }

        Assert.assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }

    @Test
    public void testIdentity() throws Exception{
        int LOOP_COUNT = 2;
        File tmpFile = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0;j<LOOP_COUNT;j+=2){
                ps.println(i+"\t"+j);
                ps.println(i+"\t"+j);
            }
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigContext) + "';");
        pigServer.registerQuery("B = distinct A ;"); //the argument does not matter
        pigServer.registerQuery("C = foreach B generate FLATTEN(" + Identity.class.getName() + "($0, $1));"); //the argument does not matter

        Iterator<Tuple> iter = pigServer.openIterator("C");
        if(!iter.hasNext()) Assert.fail("No output found");
        int numRows = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j = 0; j < LOOP_COUNT; j+=2){
                Tuple t = null;
                if(iter.hasNext()) t = iter.next();
                Assert.assertEquals(2, t.size());
                Assert.assertEquals(new Double(i), new Double(t.get(0).toString()), 0.01);
                Assert.assertEquals(new Double(j), new Double(t.get(1).toString()), 0.01);
                ++numRows;
            }
        }

        Assert.assertEquals((LOOP_COUNT * LOOP_COUNT)/2, numRows);
    }
    
    @Test
    public void testCogroupAfterDistinct() throws Exception {
        String[] input1 = {
                "abc",
                "abc",
                "def",
                "def",
                "def",
                "abc",
                "def",
                "ghi"
                };
        String[] input2 = {
            "ghi	4",
            "rst	12344",
            "uvw	1",
            "xyz	4141"
            };
        Util.createInputFile(cluster, "table1", input1);
        Util.createInputFile(cluster, "table2", input2);
        
        pigServer.registerQuery("nonuniqtable1 = LOAD 'table1' AS (f1:chararray);");
        pigServer.registerQuery("table1 = DISTINCT nonuniqtable1;");
        pigServer.registerQuery("table2 = LOAD 'table2' AS (f1:chararray, f2:int);");
        pigServer.registerQuery("temp = COGROUP table1 BY f1 INNER, table2 BY f1;");
        Iterator<Tuple> it = pigServer.openIterator("temp");
        
        // results should be:
        // (abc,{(abc)},{})
        // (def,{(def)},{})
        // (ghi,{(ghi)},{(ghi,4)})
        HashMap<String, Tuple> results = new HashMap<String, Tuple>();
        Object[] row = new Object[] { "abc",
                Util.createBagOfOneColumn(new String[] { "abc"}), mBf.newDefaultBag() };
        results.put("abc", Util.createTuple(row)); 
        row = new Object[] { "def",
                Util.createBagOfOneColumn(new String[] { "def"}), mBf.newDefaultBag() };
        results.put("def", Util.createTuple(row));
        Object[] thirdColContents = new Object[] { "ghi", 4 };
        Tuple t = Util.createTuple(thirdColContents);
        row = new Object[] { "ghi",
                Util.createBagOfOneColumn(new String[] { "ghi"}), Util.createBag(new Tuple[] { t })};
        results.put("ghi", Util.createTuple(row));

        while(it.hasNext()) {
            Tuple tup = it.next();
            List<Object> fields = tup.getAll();
            Tuple expected = results.get((String)fields.get(0));
            int i = 0;
            for (Object field : fields) {
                Assert.assertEquals(expected.get(i++), field);
            }
        }
        
        Util.deleteFile(cluster, "table1");
        Util.deleteFile(cluster, "table2");
    }

    @Test
    public void testAlgebraicDistinctProgress() throws Exception {
    
        //creating a test input of larger than 1000 to make
        //sure that progress kicks in. The only way to test this 
        //is to add a log statement to the getDistinct
        //method in Distinct.java. There is no automated mechanism
        //to check this from pig
        int inputSize = 4004;
        Integer[] inp = new Integer[inputSize];
        String[] inpString = new String[inputSize];
        for(int i = 0; i < inputSize; i+=2) {
            inp[i] = i/2;
            inp[i+1] = i/2;
            inpString[i] = new Integer(i/2).toString();
            inpString[i+1] = new Integer(i/2).toString();
        }
               
        Util.createInputFile(cluster, "table", inpString);

        pigServer.registerQuery("a = LOAD 'table' AS (i:int);");
        pigServer.registerQuery("b = group a ALL;");
        pigServer.registerQuery("c = foreach b {aa = DISTINCT a; generate COUNT(aa);};");
        Iterator<Tuple> it = pigServer.openIterator("c");
     
        Integer[] exp = new Integer[inputSize/2];
        for(int j = 0; j < inputSize/2; ++j) {
            exp[j] = j;
        }

        DataBag expectedBag = Util.createBagOfOneColumn(exp);
        
        while(it.hasNext()) {
            Tuple tup = it.next();
            Long resultBagSize = (Long)tup.get(0);
            Assert.assertTrue(DataType.compare(expectedBag.size(), resultBagSize) == 0);
        }
        
        Util.deleteFile(cluster, "table");        
    }

    @Test
    public void testBinStorageWithLargeStrings() throws Exception {
        // Create input file with large strings
    	int testSize = 100;
    	String[] stringArray = new String[testSize];
    	Random random = new Random();
    	stringArray[0] = GenRandomData.genRandLargeString(random, 65534);
    	for(int i = 1; i < stringArray.length; ++i) {
    		//generate a few large strings every 25th record
    		if((i % 25) == 0) {
    			stringArray[i] = GenRandomData.genRandLargeString(random, 65535 + i);    			
    		} else {
    			stringArray[i] = GenRandomData.genRandString(random);
    		}
    	}
        
    	Util.createInputFile(cluster, "table", stringArray);
        
    	//test with BinStorage
        pigServer.registerQuery("a = load 'table' using PigStorage() " +
                "as (c: chararray);");
        String output = "/pig/out/TestEvalPipeline-testBinStorageLargeStrings";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());
        
        pigServer.registerQuery("b = load '" + output +"' using BinStorage() " +
        "as (c:chararray);");
        pigServer.registerQuery("c = foreach b generate c;");
        
        Iterator<Tuple> it = pigServer.openIterator("c");
        int counter = 0;
        while(it.hasNext()) {
            Tuple tup = it.next();
            String resultString = (String)tup.get(0);
            String expectedString = stringArray[counter];
          	Assert.assertTrue(expectedString.equals(resultString));
            ++counter;
        }
        Util.deleteFile(cluster, "table");
    }

}
