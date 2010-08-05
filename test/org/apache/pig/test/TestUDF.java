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

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
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

	@Override
	@After
	public void tearDown() throws Exception {
		TempScriptFile.delete();
	}
}
