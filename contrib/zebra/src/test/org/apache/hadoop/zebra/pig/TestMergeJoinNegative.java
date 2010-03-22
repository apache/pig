/**
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

package org.apache.hadoop.zebra.pig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestMergeJoinNegative extends BaseTestCase {
	
	final static String STR_SCHEMA1 = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String STR_STORAGE1 = "[a, b, c]; [e, f]; [m1#{a}]";
	final static String STR_SCHEMA2 = "aa:int,bb:float,ee:string";
	final static String STR_STORAGE2 = "[aa, bb]; [ee]";
	final static String STR_SCHEMA3 = "a:string,b:int,c:float";
	final static String STR_STORAGE3 = "[a, b]; [c]";
	
	static int fileId = 0;
	
	private static Path pathTable1;
	private static Path pathTable2;
	private static Path pathTable3;
	private static Path pathTable4;
	
	@BeforeClass
	public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestMergeJoinNegative1");
    pathTable2 = getTableFullPath("TestMergeJoinNegative2"); 
    pathTable3 = getTableFullPath("TestMergeJoinNegative3");
    pathTable4 = getTableFullPath("TestMergeJoinNegative4");
    removeDir(pathTable1);
    removeDir(pathTable2);
    removeDir(pathTable3);
    removeDir(pathTable4);
    
		// Create table1 data
		Map<String, String> m1 = new HashMap<String, String>();
		m1.put("a","m1-a");
		m1.put("b","m1-b");
		
		Object[][] table1 = {
			{5,		-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),		m1},
			{-1,	3.25f,	1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),		m1},
			{1001,	100.0f,	1000L,	50e+2,	"apple",	new DataByteArray("apple"),		m1},
			{1002,	28.0f,	1000L,	50e+2,	"hadoop",	new DataByteArray("hadoop"),	m1},
			{1000,	0.0f,	1002L,	52e+2,	"apple",	new DataByteArray("apple"),		m1} };
		
		// Create table1
		createTable(pathTable1, STR_SCHEMA1, STR_STORAGE1, table1);
		
		// Create table2 data
		Map<String, String> m2 = new HashMap<String, String>();
		m2.put("a","m2-a");
		m2.put("b","m2-b");
		
		Object[][] table2 = {
			{15,	56.0f,	1004L,	50e+2,	"green",	new DataByteArray("green"),		m2},
			{-1,	-99.0f,	1008L,	51e+2,	"orange",	new DataByteArray("orange"),	m2},
			{1001,	0.0f,	1000L,	55e+2,	"white",	new DataByteArray("white"),		m2},
			{1001,	-88.0f,	1001L,	52e+2,	"brown",	new DataByteArray("brown"),		m2},
			{2000,	33.0f,	1002L,	52e+2,	"beige",	new DataByteArray("beige"),		m2} };
		
		// Create table2
		createTable(pathTable2, STR_SCHEMA1, STR_STORAGE1, table2);
		
		// Create table3 data
		Object[][] table3 = {
			{0,		7.0f,	"grape"},
			{1001,	8.0f,	"orange"},
			{-200,	9.0f,	"banana"},
			{8,		-88.0f,	"peach"} };
		
		// Create table3
		createTable(pathTable3, STR_SCHEMA2, STR_STORAGE2, table3);
		
		// Create table4 data
		Object[][] table4 = {
			{"grape",	0,		7.0f},
			{"orange",	1001,	8.0f},
			{"banana",	-200,	9.0f},
			{"peach",	8,		-88.0f} };
		
		// Create table4
		createTable(pathTable4, STR_SCHEMA3, STR_STORAGE3, table4);
		
		// Load table1
		String query1 = "table1 = LOAD '" + pathTable1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query1);
		
		// Load table2
		String query2 = "table2 = LOAD '" + pathTable2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query2);
		
		// Load table3
		String query3 = "table3 = LOAD '" + pathTable3.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query3);
		
		// Load table4
		String query4 = "table4 = LOAD '" + pathTable4.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query4);
	}
	
	private static void createTable(Path path, String schemaString, String storageString, Object[][] tableData) throws IOException {
		// Create table from tableData array
		BasicTable.Writer writer = new BasicTable.Writer(path, schemaString, storageString, conf);
		
		Schema schema = writer.getSchema();
		Tuple tuple = TypesUtils.createTuple(schema);
		TableInserter inserter = writer.getInserter("ins", false);
		
		for (int i = 0; i < tableData.length; ++i) {
			TypesUtils.resetTuple(tuple);
			for (int k = 0; k < tableData[i].length; ++k) {
				tuple.set(k, tableData[i][k]);
				System.out.println("DEBUG: setting tuple k=" + k + "value= " + tableData[i][k]);
			}
			inserter.insert(new BytesWritable(("key" + i).getBytes()), tuple);
		}
		inserter.close();
		writer.close();
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		pigServer.shutdown();
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_12() throws ExecException, IOException {
		//
		// Pig script changes position of join key (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort2 = pathTable2.toString() + Integer.toString(fileId);
		pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load2 = "records2 = LOAD '" + pathSort2 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load2);
		
		// Change position and name of join key
		String reorder1 = "reorder1 = FOREACH records1 GENERATE b as n2, a as n1, c as c, d as d, e as e, f as f, m1 as m1;";
		pigServer.registerQuery(reorder1);
		
		// Merge tables
		String join = "joinRecords = JOIN reorder1 BY " + "(" + "n2" + ")" + " , records2 BY " + "("+ "a" + ")" +
			" USING \"merge\";";    // n2 is wrong data type
		pigServer.registerQuery(join);
		
		pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_13() throws ExecException, IOException {
		//
		// Pig script changes sort order (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort2 = pathTable2.toString() + Integer.toString(fileId);
		pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load2 = "records2 = LOAD '" + pathSort2 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load2);
		
		// Change sort order for key
		String reorder1 = "reorder1 = FOREACH records1 GENERATE a*(-1) as a, b as b, c as c, d as d, e as e, f as f, m1 as m1;";
		pigServer.registerQuery(reorder1);
		
		// Merge tables
		String join = "joinRecords = JOIN reorder1 BY " + "(" + "a" + ")" + " , records2 BY " + "("+ "a" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_14() throws ExecException, IOException {
		//
		// Left hand table is not in ascending order (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "b" + " ;";  // sort left hand table by wrong key
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort2 = pathTable2.toString() + Integer.toString(fileId);
		pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load2 = "records2 = LOAD '" + pathSort2 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load2);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "a" + ")" + " , records2 BY " + "("+ "a" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_15() throws ExecException, IOException {
		//
		// Right hand table is not in ascending order (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "b" + " ;";  // sort right hand table by wrong key
		pigServer.registerQuery(orderby2);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort2 = pathTable2.toString() + Integer.toString(fileId);
		pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load2 = "records2 = LOAD '" + pathSort2 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load2);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "a" + ")" + " , records2 BY " + "("+ "a" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_16() throws ExecException, IOException {
		//
		// More than two input tables (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		String orderby3 = "sort3 = ORDER table3 BY " + "aa" + " ;";
		pigServer.registerQuery(orderby3);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort2 = pathTable2.toString() + Integer.toString(fileId);
		pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort3 = pathTable3.toString() + Integer.toString(fileId);
		pigServer.store("sort3", pathSort3, TableStorer.class.getCanonicalName() +
			"('[aa, bb]; [ee]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load2 = "records2 = LOAD '" + pathSort2 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load2);
		
		String load3 = "records3 = LOAD '" + pathSort3 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('aa, bb, ee', 'sorted');";
		pigServer.registerQuery(load3);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "a" + ")" + " , records2 BY " + "("+ "a" + ")" +
			" , records3 BY " + "("+ "aa" + ")" + " USING \"merge\";";  // merge three tables
		pigServer.registerQuery(join);
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_25() throws ExecException, IOException {
		//
		// Two tables do not have common join key (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby3 = "sort3 = ORDER table3 BY " + "aa" + " ;";
		pigServer.registerQuery(orderby3);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort3 = pathTable3.toString() + Integer.toString(fileId);
		pigServer.store("sort3", pathSort3, TableStorer.class.getCanonicalName() +
			"('[aa, bb]; [ee]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load3 = "records3 = LOAD '" + pathSort3 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('aa, bb, ee', 'sorted');";
		pigServer.registerQuery(load3);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "a" + ")" + " , records3 BY " + "("+ "a" + ")" +
			" USING \"merge\";";					// sort key a does not exist for records3
		pigServer.registerQuery(join);
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_26() throws ExecException, IOException {
		//
		// Two tables do not have common join key (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby3 = "sort3 = ORDER table3 BY " + "aa" + " ;";
		pigServer.registerQuery(orderby3);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort3 = pathTable3.toString() + Integer.toString(fileId);
		pigServer.store("sort3", pathSort3, TableStorer.class.getCanonicalName() +
			"('[aa, bb]; [ee]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load3 = "records3 = LOAD '" + pathSort3 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('aa, bb, ee', 'sorted');";
		pigServer.registerQuery(load3);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "aa" + ")" + " , records3 BY " + "("+ "aa" + ")" +
			" USING \"merge\";";					// sort key aa does not exist for records1
		pigServer.registerQuery(join);
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_27() throws ExecException, IOException {
		//
		// Two tables do not have common join key (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby3 = "sort3 = ORDER table3 BY " + "aa" + " ;";
		pigServer.registerQuery(orderby3);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort3 = pathTable3.toString() + Integer.toString(fileId);
		pigServer.store("sort3", pathSort3, TableStorer.class.getCanonicalName() +
			"('[aa, bb]; [ee]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load3 = "records3 = LOAD '" + pathSort3 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('aa, bb, ee', 'sorted');";
		pigServer.registerQuery(load3);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "aaa" + ")" + " , records3 BY " + "("+ "aaa" + ")" +
			" USING \"merge\";";					// sort key aaa does not exist for records1 and records3
		pigServer.registerQuery(join);
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_28() throws ExecException, IOException {
		//
		// Two table key names are the same but the data types are different (negative test)
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";  // a is float
		pigServer.registerQuery(orderby1);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "a" + " ;";  // a is string
		pigServer.registerQuery(orderby4);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = pathTable1.toString() + Integer.toString(fileId);
		pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b, c]; [d, e, f, m1]')");
		
		String pathSort4 = pathTable4.toString() + Integer.toString(fileId);
		pigServer.store("sort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('[a, b]; [c]')");
		
		// Load sorted tables
		String load1 = "records1 = LOAD '" + pathSort1 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, m1', 'sorted');";
		pigServer.registerQuery(load1);
		
		String load4 = "records4 = LOAD '" + pathSort4 +
			"' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c', 'sorted');";
		pigServer.registerQuery(load4);
		
		// Merge tables
		String join = "joinRecords = JOIN records1 BY " + "(" + "a" + ")" + " , records4 BY " + "("+ "a" + ")" +
			" USING \"merge\";";					// sort key a is different data type for records1 and records4
		pigServer.registerQuery(join);
		
	  pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
}
