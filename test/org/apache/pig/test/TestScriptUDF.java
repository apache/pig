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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestScriptUDF extends TestCase {
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    // See PIG-928
    @Test
    public void testPythonStandardScript() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                "\treturn (number * number)"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        Util.createInputFile(cluster, "table_testPythonStandardScript", input);
        Util.createLocalInputFile( "testPythonStandardScript.py", script);

        // Test the namespace
        pigServer.registerCode("testPythonStandardScript.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonStandardScript' as (a0:long, a1:long);");
        pigServer.registerQuery("B = foreach A generate pig.square(a0);");

        pigServer.registerCode("testPythonStandardScript.py", "jython", null);
        pigServer.registerQuery("C = foreach A generate square(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertTrue(t.toString().equals("(1)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(4)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(9)"));

        iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(1)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(4)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(9)"));
    }
    
    @Test
    public void testJavascriptExampleScript() throws Exception{
        String[] script = {
                "helloworld.outputSchema = \"word:chararray\";",
                "function helloworld() {",
                "return 'Hello, World';",
                "}",
                "complex.outputSchema = \"word:chararray,num:long\";",
                "function complex(word) {",
                "return {word:word, num:word.length};",
                "}",
        };
        String[] input = {
                "one\t1",
                "two\t2",
                "three\t3"
        };

        Util.createInputFile(cluster, "table_testJavascriptExampleScript", input);
        Util.createLocalInputFile( "testJavascriptExampleScript.js", script);

        // Test the namespace
        pigServer.registerCode("testJavascriptExampleScript.js", "javascript", "myfuncs");
        pigServer.registerQuery("A = LOAD 'table_testJavascriptExampleScript' as (a0:chararray, a1:long);");
        pigServer.registerQuery("B = foreach A generate myfuncs.helloworld(), myfuncs.complex($0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(((Tuple)t.get(1)).get(1), 3);

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(((Tuple)t.get(1)).get(1), 3);

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(((Tuple)t.get(1)).get(1), 5);

    }

    // See PIG-928
    @Test
    public void testPythonScriptWithSchemaFunction() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchemaFunction(\"squareSchema\")",
                "def square(number):" ,
                "\treturn (number * number)\n",
                "@schemaFunction(\"square\")",
                "def squareSchema(input):",
                "\treturn input "
        };
        String[] input = {
                "1\t3.0",
                "2\t4.0",
                "3\t5.0"
        };

        Util.createInputFile(cluster, "table_testPythonScriptWithSchemaFunction", input);
        Util.createLocalInputFile( "testPythonScriptWithSchemaFunction.py", script);

        // Test the namespace
        pigServer.registerCode("testPythonScriptWithSchemaFunction.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptWithSchemaFunction' as (a0:int, a1:double);");
        pigServer.registerQuery("B = foreach A generate pig.square(a0);");

        pigServer.registerCode("testPythonScriptWithSchemaFunction.py", "jython", null);
        pigServer.registerQuery("C = foreach A generate square(a1);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertTrue(t.toString().equals("(1)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(4)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(9)"));

        // The same python function will operate on double and try to get square of double
        // Since these are small double numbers we do not need to use delta to test the results
        iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(9.0)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(16.0)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(25.0)"));
    }

    // See PIG-928
    @Test
    public void testPythonScriptUDFNoDecorator() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                // No decorator means schema is null - bytearray...
                "def concat(word):" ,
                "\treturn word + word"
        };
        String[] input = {
                "hello\t1",
                "pig\t2",
                "world\t3"
        };

        Util.createInputFile(cluster, "table_testPythonScriptUDFNoDecorator", input);
        Util.createLocalInputFile( "testPythonScriptUDFNoDecorator.py", script);

        pigServer.registerCode("testPythonScriptUDFNoDecorator.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptUDFNoDecorator' as (a0, a1:int);");
        pigServer.registerQuery("B = foreach A generate pig.concat(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        // We need to check whether this is a DataByteArray or fail otherwise
        if(!(t.get(0) instanceof DataByteArray)) {
            fail("Default return type should be bytearray");
        }

        assertTrue(t.get(0).toString().trim().equals("hellohello"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.get(0).toString().trim().equals("pigpig"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.get(0).toString().trim().equals("worldworld"));
    }
    
    @Test
    public void testPythonScriptUDFBagInput() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"bag:{(y:{t:(word:chararray)}}\")",
                "def collect(bag):" ,
                "\toutBag = []",
                "\tfor word in bag:",
                // We need to wrap word inside a tuple for pig
                "\t\ttup=(len(bag), word[1])",
                "\t\toutBag.append(tup)",
                "\treturn outBag"
        };
        String[] input = {
                "1\thello",
                "2\tpig",
                "1\tworld",
                "1\tprogram",
                "2\thadoop"
        };

        Util.createInputFile(cluster, "table_testPythonScriptUDFBagInput", input);
        Util.createLocalInputFile( "testPythonScriptUDFBagInput.py", script);

        pigServer.registerCode("testPythonScriptUDFBagInput.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptUDFBagInput' as (a0:int, a1:chararray);");
        pigServer.registerQuery("B = group A by a0;");
        pigServer.registerQuery("C = foreach B generate pig.collect(A);");

        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        DataBag bag; 
        Tuple tup;
        bag = BagFactory.getInstance().newDefaultBag();
        tup = TupleFactory.getInstance().newTuple();
        tup.append(3);
        tup.append("hello");
        bag.add(tup);
        tup = TupleFactory.getInstance().newTuple();
        tup.append(3);
        tup.append("world");
        bag.add(tup);
        tup = TupleFactory.getInstance().newTuple();
        tup.append(3);
        tup.append("program");
        bag.add(tup);
    
        assertTrue(t.get(0).toString().equals(bag.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        bag = BagFactory.getInstance().newDefaultBag();
        tup = TupleFactory.getInstance().newTuple();
        tup.append(2);
        tup.append("pig");
        bag.add(tup);
        tup = TupleFactory.getInstance().newTuple();
        tup.append(2);
        tup.append("hadoop");
        bag.add(tup);
        
        assertTrue(t.get(0).toString().equals(bag.toString()));
    }
    
    @Test
    public void testPythonScriptUDFMapInput() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"bag:{(y:{t:(word:chararray)}}\")",
                "def maptobag(map):" ,
                "\toutBag = []",
                "\tfor k, v in map.iteritems():",
                // We need to wrap word inside a tuple for pig
                "\t\ttup = (k, v)",
                "\t\toutBag.append(tup)",
                "\treturn outBag"
        };
        String[] input = {
                "[1#hello,2#world]",
                "[3#pig,4#rocks]",
        };

        Util.createInputFile(cluster, "table_testPythonScriptUDFMapInput", input);
        Util.createLocalInputFile( "testPythonScriptUDFMapInput.py", script);

        pigServer.registerCode("testPythonScriptUDFMapInput.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptUDFMapInput' as (a0:map[]);");
        pigServer.registerQuery("B = foreach A generate pig.maptobag(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        DataBag bag; 
        Tuple tup;
        bag = BagFactory.getInstance().newDefaultBag();
        tup = TupleFactory.getInstance().newTuple();
        tup.append(1);
        tup.append("hello");
        bag.add(tup);
        tup = TupleFactory.getInstance().newTuple();
        tup.append(2);
        tup.append("world");
        bag.add(tup);
        assertTrue(t.get(0).toString().equals(bag.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        tup = TupleFactory.getInstance().newTuple();
        tup.append(3);
        tup.append("pig");
        assertTrue(t.toString().contains(tup.toString()));
        
        tup = TupleFactory.getInstance().newTuple();
        tup.append(4);
        tup.append("rocks");
        assertTrue(t.toString().contains(tup.toString()));
        
        assertFalse(iter.hasNext());
        
    }
    
    @Test
    public void testPythonScriptUDFMapOutput() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"mapint:[]\")",
                "def maptomapint(map):" ,
                "\toutMap = {}",
                "\tfor k, v in map.iteritems():",
                "\t\toutMap[k] = len(v)",
                "\treturn outMap"
        };
        String[] input = {
                "[1#hello,2#world]",
                "[3#pig,4#rocks]",
        };

        Util.createInputFile(cluster, "table_testPythonScriptUDFMapOutput", input);
        Util.createLocalInputFile( "testPythonScriptUDFMapOutput.py", script);

        pigServer.registerCode("testPythonScriptUDFMapOutput.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptUDFMapOutput' as (a0:map[]);");
        pigServer.registerQuery("B = foreach A generate pig.maptomapint(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertEquals(5, ((Map<?,?>)t.get(0)).get("1"));
        assertEquals(5, ((Map<?,?>)t.get(0)).get("2"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(3, ((Map<?,?>)t.get(0)).get("3"));
        assertEquals(5, ((Map<?,?>)t.get(0)).get("4"));
        
        assertFalse(iter.hasNext());
        
    }
    
    @Test
    public void testPythonScriptUDFNullInputOutput() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"bag:{(y:{t:(word:chararray)}}\")",
                "def multStr(cnt, str):" ,
                "\tif cnt != None and str != None:",
                "\t\treturn cnt * str",
                "\telse:",
                "\t\treturn None"
        };
        String[] input = {
                "3\thello",
                // Null input
                "\tworld",
        };
        
        Util.createInputFile(cluster, "table_testPythonScriptUDFNullInputOutput", input);
        Util.createLocalInputFile( "testPythonScriptUDFNullInputOutput.py", script);

        pigServer.registerCode("testPythonScriptUDFNullInputOutput.py", "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonScriptUDFNullInputOutput' as (a0:int, a1:chararray);");
        pigServer.registerQuery("B = foreach A generate pig.multStr(a0, a1);");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.get(0).toString().equals("hellohellohello"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        // UDF takes null and returns null
        assertTrue(t.get(0) == null);
        
    }
    
    // See Pig-1653
    @Test
    public void testPythonAbsolutePath() throws Exception{
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                "\treturn (number * number)"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        Util.createInputFile(cluster, "table_testPythonAbsolutePath", input);
        File scriptFile = Util.createLocalInputFile( "testPythonAbsolutePath.py", script);

        // Test the namespace
        pigServer.registerCode(scriptFile.getAbsolutePath(), "jython", "pig");
        pigServer.registerQuery("A = LOAD 'table_testPythonAbsolutePath' as (a0:long, a1:long);");
        pigServer.registerQuery("B = foreach A generate pig.square(a0);");

        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertTrue(t.toString().equals("(1)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(4)"));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.toString().equals("(9)"));
        
        assertFalse(iter.hasNext());
    }
}

