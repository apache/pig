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
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.*;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.builtin.BinStorage;

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
    public void testUdfInputOrder() throws IOException {
        String[] input = {
                "(123)",
                "((123)",
                "(123123123123)",
                "(asdf)"
        };
        
        Util.createInputFile(cluster, "table_udfInp", input);
        pigServer.registerQuery("a = load 'table_udfInp' as (i:int);");
        pigServer.registerQuery("b = foreach a {dec = 'hello'; str1 = " +  Identity.class.getName() + 
                    "(dec,'abc','def');" + 
                    "generate dec,str1; };");
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        Tuple out = (Tuple)tup.get(1);

        assertEquals( out.get(0).toString(), "hello");
        assertEquals(out.get(1).toString(), "abc");
        assertEquals(out.get(2).toString(), "def");
        
        Util.deleteFile(cluster, "table_udfInp");
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
    @Test
    public void testBinStorageByteArrayCastsSimple() throws IOException {
        // Test for PIG-544 fix
        // Tries to read data in BinStorage bytearrays as other pig types,
        // should return null if the conversion fails.
        // This test case does not use a practical example , it just tests
        // if the conversion happens when minimum conditions for conversion
        // such as expected number of bytes are met.
        String[] input = {
                    "asdf\t12\t1.1\t231\t234", 
                    "sa\t1231\t123.4\t12345678\t1234.567",
                    "asdff\t1232123\t1.45345\t123456789\t123456789.9"
                    };
        
        Util.createInputFile(cluster, "table_bs_ac", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac';");
        String output = "/pig/out/TestEvalPipeline2_BinStorageByteArrayCasts";
        pigServer.deleteFile(output);
        pigServer.store("a", output, BinStorage.class.getName());

        pigServer.registerQuery("b = load '" + output + "' using BinStorage() "
                + "as (name: int, age: int, gpa: float, lage: long, dgpa: double);");
        
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;
        
        // I have separately verified only few of the successful conversions,
        // assuming the rest are correct.
        // It is primarily testing if null is being returned when conversions
        // are expected to fail
        
        //tuple 1 
        tup = it.next();

        
        //1634952294 is integer whose  binary represtation is same as that of "asdf"
        // other columns are returning null because they have less than num of bytes
        //expected for the corresponding numeric type's binary respresentation.
        assertTrue( (Integer)tup.get(0) == 1634952294); 
        assertTrue(tup.get(1) == null);
        assertTrue(tup.get(2) == null);
        assertTrue(tup.get(3) == null);
        assertTrue(tup.get(4) == null);
        
        //tuple 2 
        tup = it.next();
        assertTrue(tup.get(0) == null);
        assertTrue( (Integer)tup.get(1) == 825373489);
        assertTrue( (Float)tup.get(2) == 2.5931501E-9F);
        assertTrue( (Long)tup.get(3) == 3544952156018063160L);
        assertTrue( (Double)tup.get(4) == 1.030084341992388E-71);
        
        //tuple 3
        tup = it.next();
        // when byte array is larger than required num of bytes for given number type
        // it uses the required bytes from beginging of byte array for conversion
        // for example 1634952294 corresponds to first 4 byptes of binary string correspnding to
        // asdff
        assertTrue((Integer)tup.get(0) == 1634952294);
        assertTrue( (Integer)tup.get(1) == 825373490);
        assertTrue( (Float)tup.get(2) == 2.5350009E-9F);
        assertTrue( (Long)tup.get(3) == 3544952156018063160L);
        assertTrue( (Double)tup.get(4) == 1.0300843656201408E-71);
        
        Util.deleteFile(cluster, "table");
    }
    @Test
    public void testBinStorageByteArrayCastsComplexBag() throws IOException {
        // Test for PIG-544 fix
        
        // Tries to read data in BinStorage bytearrays as other pig bags,
        // should return null if the conversion fails.
        
        String[] input = {
                "{(asdf)}",
                "{(2344)}",
                "{(2344}",
                "{(323423423423434)}",
                "{(323423423423434L)}",
                "{(asdff)}"
        };
        
        Util.createInputFile(cluster, "table_bs_ac_clx", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac_clx' as (f1);");
        pigServer.registerQuery("b = foreach a generate (bag{tuple(int)})f1;");
        
        Iterator<Tuple> it = pigServer.openIterator("b");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        assertTrue(tup.get(0) != null);
        
        //tuple 2 
        tup = it.next();
        assertTrue(tup.get(0) != null);
        
        //tuple 3 - malformed
        tup = it.next();
        assertTrue(tup.get(0) == null);

        //tuple 4 - integer exceeds size limit
        tup = it.next();
        assertTrue(tup.get(0) == null);

        //tuple 5 
        tup = it.next();
        assertTrue(tup.get(0) != null);

        //tuple 6
        tup = it.next();
        assertTrue(tup.get(0) != null);
        
        Util.deleteFile(cluster, "table_bs_ac_clx");
    }
    @Test
    public void testBinStorageByteArrayCastsComplexTuple() throws IOException {
        // Test for PIG-544 fix
        
        // Tries to read data in BinStorage bytearrays as other pig bags,
        // should return null if the conversion fails.
        
        String[] input = {
                "(123)",
                "((123)",
                "(123123123123)",
                "(asdf)"
        };
        
        Util.createInputFile(cluster, "table_bs_ac_clxt", input);

        // test with BinStorage
        pigServer.registerQuery("a = load 'table_bs_ac_clxt' as (t:tuple(t:tuple(i:int)));");
        Iterator<Tuple> it = pigServer.openIterator("a");
        
        Tuple tup=null;

        //tuple 1 
        tup = it.next();
        assertTrue(tup.get(0) != null);
        
        //tuple 2 -malformed tuple
        tup = it.next();
        assertTrue(tup.get(0) == null);
        
        //tuple 3 - integer exceeds size limit
        tup = it.next();
        assertTrue(tup.get(0) == null);

        //tuple 5 
        tup = it.next();
        assertTrue(tup.get(0) != null);

        Util.deleteFile(cluster, "table_bs_ac_clxt");
    }
    
    @Test
    public void testPigStorageWithCtrlChars() throws Exception {
        String[] inputData = { "hello\u0001world", "good\u0001morning", "nice\u0001day" };
        Util.createInputFile(cluster, "testPigStorageWithCtrlCharsInput.txt", inputData);
        String script = "a = load 'testPigStorageWithCtrlCharsInput.txt' using PigStorage('\u0001');" +
        		"b = foreach a generate $0, CONCAT($0, '\u0005'), $1; " +
        		"store b into 'testPigStorageWithCtrlCharsOutput.txt' using PigStorage('\u0001');" +
        		"c = load 'testPigStorageWithCtrlCharsOutput.txt' using PigStorage('\u0001') as (f1:chararray, f2:chararray, f3:chararray);";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it  = pigServer.openIterator("c");
        HashMap<String, Tuple> expectedResults = new HashMap<String, Tuple>();
        expectedResults.put("hello", (Tuple) Util.getPigConstant("('hello','hello\u0005','world')"));
        expectedResults.put("good", (Tuple) Util.getPigConstant("('good','good\u0005','morning')"));
        expectedResults.put("nice", (Tuple) Util.getPigConstant("('nice','nice\u0005','day')"));
        HashMap<String, Boolean> seen = new HashMap<String, Boolean>();
        int numRows = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String firstCol = (String) t.get(0);
            assertFalse(seen.containsKey(firstCol));
            seen.put(firstCol, true);
            assertEquals(expectedResults.get(firstCol), t);
            numRows++;
        }
        assertEquals(3, numRows);
        
    }

}
