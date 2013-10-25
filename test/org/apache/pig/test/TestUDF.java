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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.test.utils.MyUDFReturnMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestUDF {

    static String[] ScriptStatement = {
            "A = LOAD 'test/org/apache/pig/test/data/passwd' USING PigStorage();",
            "B = FOREACH A GENERATE org.apache.pig.test.utils.MyUDFReturnMap(1);" };

    static File TempScriptFile = null;

    static MiniCluster cluster = MiniCluster.buildCluster();

    @Before
    public void setUp() throws Exception {
        FileLocalizer.setInitialized(false);
        TempScriptFile = File.createTempFile("temp_jira_851", ".pig");
        FileWriter writer = new FileWriter(TempScriptFile);
        for (String line : ScriptStatement) {
            writer.write(line + "\n");
        }
        writer.close();

    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testUDFReturnMap_LocalMode() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerScript(TempScriptFile.getAbsolutePath());

        Iterator<Tuple> iterator = pig.openIterator("B");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            Map<Object, Object> result = (Map<Object, Object>) tuple.get(0);
            assertEquals(result, MyUDFReturnMap.map);
        }
    }

    @Test
    public void testUDFReturnMap_MapReduceMode() throws Exception {
        Util.createInputFile(cluster, "a.txt", new String[] { "dummy",
                "dummy" });
        FileLocalizer.deleteTempFiles();
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
        pig.registerQuery("A = LOAD 'a.txt';");
        pig.registerQuery("B = FOREACH A GENERATE org.apache.pig.test.utils.MyUDFReturnMap();");

        Iterator<Tuple> iterator = pig.openIterator("B");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            Map<Object, Object> result = (Map<Object, Object>) tuple.get(0);
            assertEquals(result, MyUDFReturnMap.map);
        }
    }

    @Test
    public void testUDFMultiLevelOutputSchema() throws Exception {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pig.registerQuery("A = LOAD 'a.txt';");
        pig.registerQuery("B = FOREACH A GENERATE org.apache.pig.test.utils.MultiLevelDerivedUDF1();");
        pig.registerQuery("C = FOREACH A GENERATE org.apache.pig.test.utils.MultiLevelDerivedUDF2();");
        pig.registerQuery("D = FOREACH A GENERATE org.apache.pig.test.utils.MultiLevelDerivedUDF3();");
        Schema s = pig.dumpSchema("B");
        assertTrue(s.getField(0).type == DataType.DOUBLE);
        s = pig.dumpSchema("C");
        assertTrue(s.getField(0).type == DataType.DOUBLE);
        s = pig.dumpSchema("D");
        assertTrue(s.getField(0).type == DataType.DOUBLE);
    }

    //see PIG-2430
    @Test
    public void testEvalFuncGetArgToFunc() throws Exception {
        String input = "udf_test_jira_2430.txt";
        Util.createLocalInputFile(input, new String[]{"1,hey"});
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.registerQuery("A = LOAD '"+input+"' USING PigStorage(',') AS (x:int,y:chararray);");
        pigServer.registerQuery("B = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs(x);");
        pigServer.registerQuery("C = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs(y);");
        pigServer.registerQuery("D = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs((long)x);");
        pigServer.registerQuery("E = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs((double)x);");
        pigServer.registerQuery("F = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs((float)x);");
        Iterator<Tuple> it = pigServer.openIterator("B");
        assertEquals(Integer.valueOf(2),(Integer)it.next().get(0));
        it = pigServer.openIterator("C");
        assertEquals(Integer.valueOf(1), (Integer)it.next().get(0));
        it = pigServer.openIterator("D");
        assertEquals(Integer.valueOf(3),(Integer)it.next().get(0));
        it = pigServer.openIterator("E");
        assertEquals(Integer.valueOf(4),(Integer)it.next().get(0));
        it = pigServer.openIterator("F");
        assertEquals(Integer.valueOf(5),(Integer)it.next().get(0));
    }

    //see PIG-2430
    @Test
    public void testNormalDefine() throws Exception {
        String input = "udf_test_jira_2430_2.txt";
        Util.createLocalInputFile(input, new String[]{"1"});
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.registerQuery("A = LOAD '"+input+"' as (x:int);");
        pigServer.registerQuery("DEFINE udftest1 org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs('1');");
        pigServer.registerQuery("DEFINE udftest2 org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs('2');");
        pigServer.registerQuery("DEFINE udftest3 org.apache.pig.test.TestUDF$UdfWithFuncSpecWithArgs('3');");
        pigServer.registerQuery("B = FOREACH A GENERATE udftest1(x), udftest2(x), udftest3(x);");
        Iterator<Tuple> its = pigServer.openIterator("B");
        Tuple t = its.next();
        assertEquals(Integer.valueOf(1),t.get(0));
        assertEquals(Integer.valueOf(2),t.get(1));
        assertEquals(Integer.valueOf(3),t.get(2));
    }

    @Test
    public void testHelperEvalFunc() throws Exception {
        String pref="org.apache.pig.test.utils.HelperEvalFuncUtils$";
        String[][] UDF = {
            {pref + "BasicSUM", pref + "AccSUM", pref + "AlgSUM", "SUM"},
            {pref + "BasicCOUNT", pref + "AccCOUNT", pref + "AlgCOUNT", "COUNT"},
            {"BasLCWC", "AccLCWC", "AlgLCWC", "5*COUNT"}
        };
        String input = "udf_test_helper_eval_func.txt";
        Util.createLocalInputFile(input, new String[]{"1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n15"});
        for (String[] udfs : UDF) {
            for (int i = 0; i < udfs.length - 1; i++) {
                String query = "DEFINE BasLCWC " + pref + "BasicLongCountWithConstructor('5');";
                query += "DEFINE AccLCWC " + pref +" AccLongCountWithConstructor('5');";
                query += "DEFINE AlgLCWC " + pref + "AlgLongCountWithConstructor('5');";
                query += "A = load '" + input + "' as (x:int);";
                query += "B = foreach (group A all) generate ";
                for (String s : Arrays.copyOfRange(udfs, i, udfs.length - 1)) {
                    query += s + "(A),";
                }
                query += udfs[udfs.length - 1] + "(A);";
                PigServer pigServer = new PigServer(ExecType.LOCAL);
                pigServer.registerQuery(query);
                Iterator<Tuple> it = pigServer.openIterator("B");
                while (it.hasNext()) {
                    Tuple t = it.next();
                    Long val = (Long)t.get(0);
                    for (int j = 1; j < i; j++) {
                        assertEquals(val, t.get(j));
                    }
                }
            }
        }
    }

    @Test
    public void testEnsureProperSchema1() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery("DEFINE goodSchema1 org.apache.pig.test.TestUDF$MirrorSchema('a:int');");
        pig.registerQuery("DEFINE goodSchema2 org.apache.pig.test.TestUDF$MirrorSchema('t:(a:int, b:int, c:int)');");
        pig.registerQuery("DEFINE goodSchema3 org.apache.pig.test.TestUDF$MirrorSchema('b:{(a:int, b:int, c:int)}');");
        pig.registerQuery("a = load 'thing';");
        pig.registerQuery("b = foreach a generate goodSchema1();");
        pig.registerQuery("c = foreach a generate goodSchema2();");
        pig.registerQuery("d = foreach a generate goodSchema3();");
        pig.dumpSchema("b");
        pig.dumpSchema("c");
        pig.dumpSchema("d");
    }
    
    @Test
    public void testEvalFuncGetVarArgToFunc() throws Exception {
        String input = "udf_test_jira_3444.txt";
        Util.createLocalInputFile(input, new String[]{"dummy"});
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        pigServer.registerQuery("A = LOAD '"+input+"' USING PigStorage(',') AS (x:chararray);");
        pigServer.registerQuery("B = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithVarArgs(3);");
        pigServer.registerQuery("C = FOREACH A GENERATE org.apache.pig.test.TestUDF$UdfWithFuncSpecWithVarArgs(1,2,3,4);");

        Iterator<Tuple> it = pigServer.openIterator("B");
        assertEquals(Integer.valueOf(3),(Integer)it.next().get(0));
        it = pigServer.openIterator("C");
        assertEquals(Integer.valueOf(10), (Integer)it.next().get(0));
    }

    @Test(expected = FrontendException.class)
    public void testEnsureProperSchema2() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery("DEFINE badSchema org.apache.pig.test.TestUDF$MirrorSchema('a:int, b:int, c:int');");
        pig.registerQuery("a = load 'thing';");
        pig.registerQuery("b = foreach a generate badSchema();");
        pig.dumpSchema("b");
    }

    public static class MirrorSchema extends EvalFunc<Object> {
        private String schemaString;
        private Schema schema;

        public MirrorSchema(String schemaString) {
            this.schemaString = schemaString;
            try {
                schema = Utils.getSchemaFromString(schemaString);
            } catch (ParserException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object exec(Tuple input) throws IOException {
            return schemaString;
        }

        public Schema outputSchema(Schema input) {
            return schema;
        }
    }

    @After
    public void tearDown() throws Exception {
        TempScriptFile.delete();
    }

    public static class UdfWithFuncSpecWithArgs extends EvalFunc<Integer> {
        private Integer ret = 0;
        public UdfWithFuncSpecWithArgs() {}
        public UdfWithFuncSpecWithArgs(String ret) {
            this.ret=Integer.parseInt(ret);
        }

        @Override
        public Integer exec(Tuple input) throws IOException {
            return ret;
        }

        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> l = new ArrayList<FuncSpec>(5);
            l.add(new FuncSpec(this.getClass().getName(), new String[]{"1"}, new Schema(new Schema.FieldSchema(null,DataType.CHARARRAY))));
            l.add(new FuncSpec(this.getClass().getName(), new String[]{"2"}, new Schema(new Schema.FieldSchema(null,DataType.INTEGER))));
            l.add(new FuncSpec(this.getClass().getName(), new String[]{"3"}, new Schema(new Schema.FieldSchema(null,DataType.LONG))));
            l.add(new FuncSpec(this.getClass().getName(), new String[]{"4"}, new Schema(new Schema.FieldSchema(null,DataType.DOUBLE))));
            l.add(new FuncSpec(this.getClass().getName(), new String[]{"5"}, new Schema(new Schema.FieldSchema(null,DataType.FLOAT))));
            return l;
        }
    }
    
    public static class UdfWithFuncSpecWithVarArgs extends EvalFunc<Integer> {
        public UdfWithFuncSpecWithVarArgs() {}

        @Override
        public Integer exec(Tuple input) throws IOException {
            int res = 0;
            if (input == null || input.size() == 0) {
                return res;
            }
            for (int i = 0; i < input.size(); i++) {
                res += (Integer)input.get(i);
            }
            return res;
        }
        
        @Override
        public SchemaType getSchemaType() {
            return SchemaType.VARARG;
        }

        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> l = new ArrayList<FuncSpec>();
            Schema s1 = new Schema(new Schema.FieldSchema(null,DataType.INTEGER));
            l.add(new FuncSpec(this.getClass().getName(), s1));
            return l;
        }
    }
}