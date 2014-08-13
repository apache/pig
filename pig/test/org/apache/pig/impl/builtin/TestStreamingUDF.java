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
package org.apache.pig.impl.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.joda.time.DateTime;
import org.junit.Test;

public class TestStreamingUDF {
    private static PigServer pigServerLocal = null;
    private static PigServer pigServerMapReduce = null;
    
    private TupleFactory tf = TupleFactory.getInstance();
    private static MiniCluster cluster = MiniCluster.buildCluster();
    
    @Test
    public void testPythonUDF_onCluster() throws Exception {
        pigServerMapReduce = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'c:chararray\')",
                "def py_func(one,two):",
                "   return one + two"
        };
        
        Util.createLocalInputFile("pyfile_mr.py", pythonScript);

        String[] input = {
            "field10\tfield11",
            "field20\tfield21"
        };
        Util.createLocalInputFile("input_mr", input);
        Util.copyFromLocalToCluster(cluster, "input_mr", "input_mr");

        pigServerMapReduce.registerQuery("REGISTER 'pyfile_mr.py' USING streaming_python AS pf;");
        pigServerMapReduce.registerQuery("A = LOAD 'input_mr' as (c1:chararray, c2:chararray);");
        pigServerMapReduce.registerQuery("B = FOREACH A generate pf.py_func(c1, c2);");

        Iterator<Tuple> iter = pigServerMapReduce.openIterator("B");
        assertTrue(iter.hasNext());
        Tuple actual0 = iter.next();
        assertTrue(iter.hasNext());
        Tuple actual1 = iter.next();

        Tuple expected0 = tf.newTuple("field10field11");
        Tuple expected1 = tf.newTuple("field20field21");

        assertEquals(expected0, actual0);
        assertEquals(expected1, actual1);
    }

    @Test
    public void testPythonUDF() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'c:chararray\')",
                "def py_func(one,two):",
                "   return one + two"
        };
        Util.createLocalInputFile( "pyfile.py", pythonScript);

        
        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(2);
        t0.set(0, "field10");
        t0.set(1, "field11");
        Tuple t1 = tf.newTuple(2);
        t1.set(0, "field20");
        t1.set(1, "field21");
        data.set("testTuples", "c1:chararray,c2:chararray", t0, t1);
        
        pigServerLocal.registerQuery("REGISTER 'pyfile.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("A = LOAD 'testTuples' USING mock.Storage();");
        pigServerLocal.registerQuery("B = FOREACH A generate pf.py_func(c1, c2);");
        pigServerLocal.registerQuery("STORE B INTO 'out' USING mock.Storage();");
        
        Tuple expected0 = tf.newTuple("field10field11");
        Tuple expected1 = tf.newTuple("field20field21");
        
        List<Tuple> out = data.get("out");
        assertEquals(expected0, out.get(0));
        assertEquals(expected1, out.get(1));
    }
    
    @Test
    public void testPythonUDF_withNewline() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'c:chararray\')",
                "def py_func(one,two):",
                "   return '%s\\n%s' % (one,two)"
        };
        Util.createLocalInputFile( "pyfileNL.py", pythonScript);

        
        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(2);
        t0.set(0, "field10");
        t0.set(1, "field11");
        Tuple t1 = tf.newTuple(2);
        t1.set(0, "field20");
        t1.set(1, "field21");
        data.set("testTuplesNL", "c1:chararray,c2:chararray", t0, t1);
        
        pigServerLocal.registerQuery("REGISTER 'pyfileNL.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("A = LOAD 'testTuplesNL' USING mock.Storage();");
        pigServerLocal.registerQuery("B = FOREACH A generate pf.py_func(c1, c2);");
        pigServerLocal.registerQuery("STORE B INTO 'outNL' USING mock.Storage();");
        
        Tuple expected0 = tf.newTuple("field10\nfield11");
        Tuple expected1 = tf.newTuple("field20\nfield21");
        
        List<Tuple> out = data.get("outNL");
        assertEquals(expected0, out.get(0));
        assertEquals(expected1, out.get(1));
    }
    
    @Test
    public void testPythonUDF__withBigInteger() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'biout:biginteger\')",
                "def py_func(bi):",
                "   return bi"
        };
        Util.createLocalInputFile( "pyfile_bi.py", pythonScript);

        
        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(new BigInteger("123456789012345678901234567890"));
        Tuple t1 = tf.newTuple(new BigInteger("9123456789012345678901234567890"));
        data.set("testBiTuples", "bi:biginteger", t0, t1);

        pigServerLocal.registerQuery("REGISTER 'pyfile_bi.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("A = LOAD 'testBiTuples' USING mock.Storage();");
        pigServerLocal.registerQuery("B = FOREACH A generate pf.py_func(bi);");
        pigServerLocal.registerQuery("STORE B INTO 'bi_out' USING mock.Storage();");
        
        List<Tuple> out = data.get("bi_out");
        assertEquals(t0, out.get(0));
        assertEquals(t1, out.get(1));
    }
    
    @Test
    public void testPythonUDF__withBigDecimal() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'bdout:bigdecimal\')",
                "def py_func(bd):",
                "   return bd"
        };
        Util.createLocalInputFile( "pyfile_bd.py", pythonScript);

        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(new BigDecimal("123456789012345678901234567890.12345"));
        Tuple t1 = tf.newTuple(new BigDecimal("9123456789012345678901234567890.12345"));
        data.set("testBdTuples", "bd:bigdecimal", t0, t1);

        pigServerLocal.registerQuery("REGISTER 'pyfile_bd.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("A = LOAD 'testBdTuples' USING mock.Storage();");
        pigServerLocal.registerQuery("B = FOREACH A generate pf.py_func(bd);");
        pigServerLocal.registerQuery("STORE B INTO 'bd_out' USING mock.Storage();");
        
        //We lose precision when we go to python.
        List<Tuple> out = data.get("bd_out");
        Float e0 = ((BigDecimal)t0.get(0)).floatValue();
        Float e1 = ((BigDecimal)t1.get(0)).floatValue();
        assertEquals(e0, ((BigDecimal) out.get(0).get(0)).floatValue(), 0.1);
        assertEquals(e1, ((BigDecimal) out.get(1).get(0)).floatValue(), 0.1);
    }
    
    @Test
    public void testPythonUDF__withDateTime() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
                "from pig_util import outputSchema",
                "@outputSchema(\'d:datetime\')",
                "def py_func(dt):",
                "   return dt"
        };
        Util.createLocalInputFile( "pyfile_dt.py", pythonScript);

        
        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(new DateTime());
        Tuple t1 = tf.newTuple(new DateTime());
        data.set("testDateTuples", "d:datetime", t0, t1);

        pigServerLocal.registerQuery("REGISTER 'pyfile_dt.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("A = LOAD 'testDateTuples' USING mock.Storage();");
        pigServerLocal.registerQuery("B = FOREACH A generate pf.py_func(d);");
        pigServerLocal.registerQuery("STORE B INTO 'date_out' USING mock.Storage();");
        
        List<Tuple> out = data.get("date_out");
        assertEquals(t0, out.get(0));
        assertEquals(t1, out.get(1));
    }
    
    @Test
    public void testPythonUDF__allTypes() throws Exception {
        pigServerLocal = new PigServer(ExecType.LOCAL);

        String[] pythonScript = {
            "# -*- coding: utf-8 -*-",
            "from pig_util import outputSchema",
            "import sys",
            "",
            "@outputSchema('tuple_output:tuple(nully:chararray, inty:int, longy:long, floaty:float, doubly:double, chararrayy:chararray, utf_chararray_basic_string:chararray, utf_chararray_unicode:chararray, bytearrayy:bytearray)')",
            "def get_tuple_output():",
            "    result = (None, 32, 1000000099990000L, 32.0, 3200.1234678509, 'Some String', 'Hello\\u2026Hello', u'Hello\\u2026Hello', b'Some Byte Array')",
            "    return result",
            "",
            "@outputSchema('tuple_output:tuple(nully:chararray, inty:int, longy:long, floaty:float, doubly:double, chararrayy:chararray, utf_chararray_basic_string:chararray, utf_chararray_unicode:chararray, bytearrayy:bytearray)')",
            "def crazy_tuple_identity(ct):",
            "    print ct",
            "    return ct",
            "",
            "@outputSchema('mappy:map[]')",
            "def add_map():",
            "    return {u'Weird\\u2026Name' : u'Weird \\u2026 Value',",
            "            'SomeNum'          : 32,",
            "            'Simple Name'      : 'Simple Value' }",
            "",
            "",
            "@outputSchema('silly:chararray')",
            "def silly(silly_word):",
            "    print silly_word.encode('utf-8')",
            "    return silly_word",
            "",
            "",
            "@outputSchema('final_output:bag{t:tuple(user_id:chararray,age:int,tuple_output:tuple(nully:chararray,inty:int,longy:long,floaty:float,doubly:double,chararrayy:chararray,utf_chararray_basic_string:chararray,utf_chararray_unicode:chararray,bytearrayy:bytearray),mappy:map[],silly:chararray)}')",
            "def bag_identity(bag):",
            "    return bag"
        };
        Util.createLocalInputFile("allfile.py", pythonScript);

        
        Data data = resetData(pigServerLocal);
        Tuple t0 = tf.newTuple(2);
        t0.set(0, "user1");
        t0.set(1, 10);
        Tuple t1 = tf.newTuple(2);
        t1.set(0, "user2");
        t1.set(1, 11);
        data.set("userTuples", "user_id:chararray,age:int", t0, t1);
        
        
        pigServerLocal.registerQuery("REGISTER 'allfile.py' USING streaming_python AS pf;");
        pigServerLocal.registerQuery("users = LOAD 'userTuples' USING mock.Storage();");
        pigServerLocal.registerQuery("crazy_tuple = FOREACH users GENERATE user_id, age, pf.get_tuple_output();");
        pigServerLocal.registerQuery("crazy_tuple_fun = FOREACH crazy_tuple GENERATE user_id, age, pf.crazy_tuple_identity(tuple_output);");
        pigServerLocal.registerQuery("crazy_tuple_with_map = FOREACH crazy_tuple_fun GENERATE user_id, age, tuple_output, pf.add_map(), pf.silly('\u2026');");
        pigServerLocal.registerQuery("crazy_group = GROUP crazy_tuple_with_map BY (age);");
        pigServerLocal.registerQuery("out = FOREACH crazy_group GENERATE group, pf.bag_identity(crazy_tuple_with_map);");

        pigServerLocal.registerQuery("STORE out INTO 'all_out' USING mock.Storage();");
        
        List<Tuple> out = data.get("all_out");

        /*
         * Expected output for first tuple.
         * (10,
         * {(user1,10,(,32,1000000099990000,32.0,3200.12346785,Some String,Hello\u2026Hello,Hello\u2026Hello,Some Byte Array),
         *   [Simple Name#Simple Value,SomeNum#32,Weird\u2026Name#Weird \u2026 Value],\u2026)
         * })
         */
        
        //Get Bag
        DataBag bag = (DataBag) out.get(0).get(1);
        assertEquals(1, bag.size());
        
        //Get First Bag Tuple
        Tuple innerTuple = bag.iterator().next();
        assertEquals(5, innerTuple.size());
        
        //Check one field in innermost tuple
        assertEquals("Hello\\u2026Hello", ((Tuple) innerTuple.get(2)).get(6));
    }
}
