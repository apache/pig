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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.MyUDFReturnMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestUDF extends TestCase {

	static String[] ScriptStatement = {
			"A = LOAD 'test/org/apache/pig/test/data/passwd' USING PigStorage();",
			"B = FOREACH A GENERATE org.apache.pig.test.utils.MyUDFReturnMap(1);" };

	static File TempScriptFile = null;

	static MiniCluster cluster = MiniCluster.buildCluster();

	@Override
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
	public void testUDFReturnMap_LocalMode() {
		try {
			PigServer pig = new PigServer(ExecType.LOCAL);
			pig.registerScript(TempScriptFile.getAbsolutePath());

			Iterator<Tuple> iterator = pig.openIterator("B");
			int index = 0;
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				index++;
				Map<Object, Object> result = (Map<Object, Object>) tuple.get(0);
				assertEquals(result, MyUDFReturnMap.map);
			}
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testUDFReturnMap_MapReduceMode() {
		try {
			Util.createInputFile(cluster, "a.txt", new String[] { "dummy",
					"dummy" });
			FileLocalizer.deleteTempFiles();
			PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster
					.getProperties());
			pig.registerQuery("A = LOAD 'a.txt';");
			pig
					.registerQuery("B = FOREACH A GENERATE org.apache.pig.test.utils.MyUDFReturnMap();");

			Iterator<Tuple> iterator = pig.openIterator("B");
			int index = 0;
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				index++;
				Map<Object, Object> result = (Map<Object, Object>) tuple.get(0);
				assertEquals(result, MyUDFReturnMap.map);
			}
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testUDFMultiLevelOutputSchema() {
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    //see PIG-2430
    @Test
    public void testEvalFuncGetArgToFunc() throws Throwable {
        String input = "udf_test_jira_2430.txt";
        Util.createLocalInputFile(input, new String[]{"1,hey"});
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    //see PIG-2430
    @Test
    public void testNormalDefine() throws Throwable {
        String input = "udf_test_jira_2430_2.txt";
        Util.createLocalInputFile(input, new String[]{"1"});
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
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

	@Override
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
}
