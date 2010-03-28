
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.StringTokenizer;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestOrderPreserveVariableTable extends BaseTestCase {
	
	final static String TABLE1_SCHEMA = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String TABLE1_STORAGE = "[a, b, c]; [d, e, f]; [m1#{a}]";
	
	final static String TABLE2_SCHEMA = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String TABLE2_STORAGE = "[a, b, c]; [d, e, f]; [m1#{a}]";
	
	final static String TABLE3_SCHEMA = "b:float,d:double,c:long,e:string,f:bytes,int1:int";
	final static String TABLE3_STORAGE = "[b,d]; [c]; [e]; [f,int1]";
	
	final static String TABLE4_SCHEMA = "e:string,str3:string,str4:string";
	final static String TABLE4_STORAGE = "[e,str3,str4]";
	
	static int fileId = 0;
	static int sortId = 0;
	
	protected static ExecJob pigJob;
	
	private static Path pathTable1;
	private static Path pathTable2;
	private static Path pathTable3;
	private static Path pathTable4;
	private static HashMap<String, String> tableStorage;
	
	private static Object[][] table1;
	private static Object[][] table2;
	private static Object[][] table3;
	private static Object[][] table4;
	
	private static Map<String, String> m1;
	private static Map<String, String> m2;

  @BeforeClass
  public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestOrderPreserveVariableTable1");
    pathTable2 = getTableFullPath("TestOrderPreserveVariableTable2");
    pathTable3 = getTableFullPath("TestOrderPreserveVariableTable3");
    pathTable4 = getTableFullPath("TestOrderPreserveVariableTable4");
  
    removeDir(pathTable1);
    removeDir(pathTable2);
    removeDir(pathTable3);
    removeDir(pathTable4);
    
		// Create table1 data
		m1 = new HashMap<String, String>();
		m1.put("a","m1-a");
		m1.put("b","m1-b");
		
		table1 = new Object[][]{
			{5,		-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),		m1},
			{-1,	3.25f,	1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),		m1},
			{1001,	100.0f,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),		m1},
			{1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),		m1},
			{1001,	50.0f,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),		m1},
			{1001,	52.0f,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),		m1},
			{1002,	28.0f,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),	m1},
			{1000,	0.0f,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),	m1} };
		
		// Create table1
		createTable(pathTable1, TABLE1_SCHEMA, TABLE1_STORAGE, table1);
		
		// Create table2 data
		m2 = new HashMap<String, String>();
		m2.put("a","m2-a");
		m2.put("b","m2-b");
		
		table2 = new Object[][] {
			{15,	56.0f,	1004L,	50e+2,	"green",	new DataByteArray("green"),		m2},
			{-1,	-99.0f,	1002L,	51e+2,	"orange",	new DataByteArray("orange"),	m2},
			{1001,	100.0f,	1003L,	55e+2,	"white",	new DataByteArray("white"),		m2},
			{1001,	102.0f,	1001L,	52e+2,	"purple",	new DataByteArray("purple"),	m2},
			{1001,	50.0f,	1008L,	52e+2,	"gray",		new DataByteArray("gray"),		m2},
			{1001,	53.0f,	1001L,	52e+2,	"brown",	new DataByteArray("brown"),		m2},
			{2000,	33.0f,	1006L,	52e+2,	"beige",	new DataByteArray("beige"),		m2} };
		
		// Create table2
		createTable(pathTable2, TABLE2_SCHEMA, TABLE2_STORAGE, table2);
		
		// Create table3 data
		table3 = new Object[][] {
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100}, };
		
		// Create table3
		createTable(pathTable3, TABLE3_SCHEMA, TABLE3_STORAGE, table3);
		
		// Create table4 data
		table4 = new Object[][] {
			{"a 8",	"Cupertino",	"California"},
			{"a 7",	"San Jose",		"California"},
			{"a 6",	"Santa Cruz",	"California"},
			{"a 5",	"Las Vegas",	"Nevada"},
			{"a 4",	"New York",		"New York"},
			{"a 3",	"Phoenix",		"Arizona"},
			{"a 2",	"Dallas",		"Texas"},
			{"a 1",	"Reno",			"Nevada"} };
		
		// Create table4
		createTable(pathTable4, TABLE4_SCHEMA, TABLE4_STORAGE, table4);
		
		// Load tables
		String query1 = "table1 = LOAD '" + pathTable1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query1);
		String query2 = "table2 = LOAD '" + pathTable2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query2);
		String query3 = "table3 = LOAD '" + pathTable3.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query3);
		String query4 = "table4 = LOAD '" + pathTable4.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query4);
		
		// Map for table storage
		tableStorage = new HashMap<String, String>();
		tableStorage.put("table1", TABLE1_STORAGE);
		tableStorage.put("table2", TABLE2_STORAGE);
		tableStorage.put("table3", TABLE3_STORAGE);
		tableStorage.put("table4", TABLE4_STORAGE);
	}
	
	private static void createTable(Path path, String schemaString, String storageString, Object[][] tableData)
			throws IOException {
		//
		// Create table from tableData array
		//
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
	
	@Test
	public void test_union_empty_left_table() throws ExecException, IOException {
		//
		// Test sorted union with two tables and left table is empty
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a,b" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a,b" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Filter to remove all rows in table
		String filter = "filtersort1 = FILTER sort1 BY a == -99 ;";
		
		pigServer.registerQuery(filter);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("filtersort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + "2";
		pigJob = pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + TABLE2_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,source_table,b,c,d,e,f,m1', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, -1,	1,	-99.0f,	1002L,	51e+2,	"orange",	new DataByteArray("orange"),m2);
		addResultRow(rows, 15,	1,	56.0f,	1004L,	50e+2,	"green",	new DataByteArray("green"),	m2);
		addResultRow(rows, 1001,	1,	100.0f,	1003L,	55e+2,	"white",	new DataByteArray("white"),	m2);
		addResultRow(rows, 1001,	1,	102.0f,	1001L,	52e+2,	"purple",	new DataByteArray("purple"),m2);
		
		addResultRow(rows, 1001,	1,	50.0f,	1008L,	52e+2,	"gray",		new DataByteArray("gray"),	m2);
		addResultRow(rows, 1001,	1,	53.0f,	1001L,	52e+2,	"brown",	new DataByteArray("brown"),	m2);
		addResultRow(rows, 2000,	1,	33.0f,	1006L,	52e+2,	"beige",	new DataByteArray("beige"),	m2);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table2.length);
	}
	
	@Test
	public void test_union_empty_right_table() throws ExecException, IOException {
		//
		// Test sorted union with two tables and right table is empty
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		// Filter to remove all rows in table
		String filter = "filtersort4 = FILTER sort4 BY e == 'teststring' ;";
		
		pigServer.registerQuery(filter);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + "4";
		pigJob = pigServer.store("filtersort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",	0,	1001,	100.0f,	1003L,	50e+2,	new DataByteArray("Apple"),	m1,	null,	null);
		addResultRow(rows, "Hadoop",	0,	1002,	28.0f,	1000L,	50e+2,	new DataByteArray("Hadoop"),m1,	null,	null);
		addResultRow(rows, "Pig",	0,	1001,	50.0f,	1000L,	50e+2,	new DataByteArray("Pig"),	m1,	null,	null);
		addResultRow(rows, "Zebra",	0,	5,		-3.25f,	1001L,	51e+2,	new DataByteArray("Zebra"),	m1,	null,	null);
		
		addResultRow(rows, "apple",	0,	1001,	101.0f,	1001L,	50e+2,	new DataByteArray("apple"),	m1,	null,	null);
		addResultRow(rows, "hadoop",	0,	1000,	0.0f,	1002L,	52e+2,	new DataByteArray("hadoop"),m1,	null,	null);
		addResultRow(rows, "pig",	0,	1001,	52.0f,	1001L,	50e+2,	new DataByteArray("pig"),	m1,	null,	null);
		addResultRow(rows, "zebra",	0,	-1,		3.25f,	1000L,	50e+2,	new DataByteArray("zebra"),	m1,	null,	null);
		resultTable.put(0, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table1.length);
	}
	
	@Test
	public void test_union_empty_middle_table() throws ExecException, IOException {
		//
		// Test sorted union with three tables and middle table is empty
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "e" + " ;";
		pigServer.registerQuery(orderby2);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		// Filter to remove all rows in table
		String filter = "filtersort2 = FILTER sort2 BY e == 'teststring' ;";
		
		pigServer.registerQuery(filter);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + "2";
		pigJob = pigServer.store("filtersort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + TABLE2_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + "4";
		pigJob = pigServer.store("sort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",  0,  1001, 100.0f, 1003L,  50e+2,  new DataByteArray("Apple"), m1, null, null);
    addResultRow(rows, "Hadoop", 0,  1002, 28.0f,  1000L,  50e+2,  new DataByteArray("Hadoop"),m1, null, null);
    addResultRow(rows, "Pig",  0,  1001, 50.0f,  1000L,  50e+2,  new DataByteArray("Pig"), m1, null, null);
    addResultRow(rows, "Zebra",  0,  5,    -3.25f, 1001L,  51e+2,  new DataByteArray("Zebra"), m1, null, null);
    
    addResultRow(rows, "apple",  0,  1001, 101.0f, 1001L,  50e+2,  new DataByteArray("apple"), m1, null, null);
    addResultRow(rows, "hadoop", 0,  1000, 0.0f, 1002L,  52e+2,  new DataByteArray("hadoop"),m1, null, null);
    addResultRow(rows, "pig",  0,  1001, 52.0f,  1001L,  50e+2,  new DataByteArray("pig"), m1, null, null);
    addResultRow(rows, "zebra",  0,  -1,   3.25f,  1000L,  50e+2,  new DataByteArray("zebra"), m1, null, null);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a 1",	2,	null,	null,	null,	null,	null,	null,	"Reno",		"Nevada");
		addResultRow(rows, "a 2",	2,	null,	null,	null,	null,	null,	null,	"Dallas",	"Texas");
		addResultRow(rows, "a 3",	2,	null,	null,	null,	null,	null,	null,	"Phoenix",	"Arizona");
		addResultRow(rows, "a 4",	2,	null,	null,	null,	null,	null,	null,	"New York",	"New York");
		
		addResultRow(rows, "a 5",	2,	null,	null,	null,	null,	null,	null,	"Las Vegas","Nevada");
		addResultRow(rows, "a 6",	2,	null,	null,	null,	null,	null,	null,	"Santa Cruz","California");
		addResultRow(rows, "a 7",	2,	null,	null,	null,	null,	null,	null,	"San Jose",	"California");
		addResultRow(rows, "a 8",	2,	null,	null,	null,	null,	null,	null,	"Cupertino","California");
		resultTable.put(2, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table1.length + table4.length);
	}
	
	@Test
	public void test_union_empty_many_table() throws ExecException, IOException {
		//
		// Test sorted union with three tables with left and right table empty
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "e" + " ;";
		pigServer.registerQuery(orderby2);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		// Filter to remove all rows in table
		String filter1 = "filtersort1 = FILTER sort1 BY e == 'teststring' ;";
		pigServer.registerQuery(filter1);
		
		// Filter to remove all rows in table
		String filter4 = "filtersort4 = FILTER sort4 BY e == 'teststring' ;";
		pigServer.registerQuery(filter4);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("filtersort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + "2";
		pigJob = pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + TABLE2_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + "4";
		pigJob = pigServer.store("filtersort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "beige",	1,	2000,	33.0f,	1006L,	52e+2,	new DataByteArray("beige"),	m2, null, null);
		addResultRow(rows, "brown",	1,	1001,	53.0f,	1001L,	52e+2,	new DataByteArray("brown"),	m2, null, null);
		addResultRow(rows, "gray",	1,	1001,	50.0f,	1008L,	52e+2,	new DataByteArray("gray"),	m2, null, null);
		addResultRow(rows, "green",	1,	15,		56.0f,	1004L,	50e+2,	new DataByteArray("green"),	m2, null, null);
		
		addResultRow(rows, "orange",	1,	-1,		-99.0f,	1002L,	51e+2,	new DataByteArray("orange"),m2, null, null);
		addResultRow(rows, "purple",	1,	1001,	102.0f,	1001L,	52e+2,	new DataByteArray("purple"),m2, null, null);
		addResultRow(rows, "white",	1,	1001,	100.0f,	1003L,	55e+2,	new DataByteArray("white"),	m2, null, null);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table2.length);
	}
	
	@Test
	public void test_union_empty_all_table() throws ExecException, IOException {
		//
		// Test sorted union with two tables and both are empty
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		// Filter to remove all rows in table
		String filter1 = "filtersort1 = FILTER sort1 BY e == 'teststring' ;";
		pigServer.registerQuery(filter1);
		
		// Filter to remove all rows in table
		String filter4 = "filtersort4 = FILTER sort4 BY e == 'teststring' ;";
		pigServer.registerQuery(filter4);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("filtersort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + "4";
		pigJob = pigServer.store("filtersort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		int numbRows = printTable("records1");
		
		Assert.assertEquals(numbRows, 0);
	}
	
	@Test
	public void test_pig_statements() throws ExecException, IOException {
		//
		// Test sorted union with two tables after pig filter and pig foreach statements
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		// Table after pig filter
		String filter1 = "filtersort1 = FILTER sort1 BY a == 1001 ;";
		pigServer.registerQuery(filter1);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("filtersort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + "4";
		pigJob = pigServer.store("sort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",  0,  1001, 100.0f, 1003L,  50e+2,  new DataByteArray("Apple"), m1, null, null);
    addResultRow(rows, "Pig",  0,  1001, 50.0f,  1000L,  50e+2,  new DataByteArray("Pig"), m1, null, null);
		addResultRow(rows, "apple",	0,	1001,	101.0f,	1001L,	50e+2,	new DataByteArray("apple"),	m1,	null,	null);
		addResultRow(rows, "pig",	0,	1001,	52.0f,	1001L,	50e+2,	new DataByteArray("pig"),	m1,	null,	null);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a 1",	1,	null,	null,	null,	null,	null,	null,	"Reno",		"Nevada");
		addResultRow(rows, "a 2",	1,	null,	null,	null,	null,	null,	null,	"Dallas",	"Texas");
		
		addResultRow(rows, "a 3",	1,	null,	null,	null,	null,	null,	null,	"Phoenix",	"Arizona");
		addResultRow(rows, "a 4",	1,	null,	null,	null,	null,	null,	null,	"New York",	"New York");
		addResultRow(rows, "a 5",	1,	null,	null,	null,	null,	null,	null,	"Las Vegas","Nevada");
		addResultRow(rows, "a 6",	1,	null,	null,	null,	null,	null,	null,	"Santa Cruz","California");
		
		addResultRow(rows, "a 7",	1,	null,	null,	null,	null,	null,	null,	"San Jose",	"California");
		addResultRow(rows, "a 8",	1,	null,	null,	null,	null,	null,	null,	"Cupertino","California");
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(12, numbRows);
	}
	
	@Test
	public void test_union_as_input() throws ExecException, IOException {
		//
		// Test sorted union with two tables, one of the tables is from previous sorted union
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "e" + " ;";
		pigServer.registerQuery(orderby2);
		
		String orderby4 = "sort4 = ORDER table4 BY " + "e" + " ;";
		pigServer.registerQuery(orderby4);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + ++fileId;
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + ++fileId;
		pigJob = pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + TABLE2_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort4 = newPath.toString() + ++fileId;
		pigJob = pigServer.store("sort4", pathSort4, TableStorer.class.getCanonicalName() +
			"('" + TABLE4_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		// Create first sorted union
		String queryLoad1 = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,a,b,c,d,f,m1', 'sorted');";
		pigServer.registerQuery(queryLoad1);
		
		// Add sort info to union table before store
		String sortunion = "sortrecords1 = ORDER records1 BY " + "e" + " ;";
		pigServer.registerQuery(sortunion);
		
		// Store union table
		String pathUnion1 = newPath.toString() + ++fileId;
		pigJob = pigServer.store("sortrecords1", pathUnion1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		// Create second sorted union
		String queryLoad2 = "records2 = LOAD '"
	        + pathUnion1 + ","
	        + pathSort4
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,str3,str4', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad2);
		pigServer.registerQuery(queryLoad2);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",  0,  1001, 100.0f, null, null);
    addResultRow(rows, "Hadoop", 0,  1002, 28.0f,  null, null);
    addResultRow(rows, "Pig",  0,  1001, 50.0f,  null, null);
    addResultRow(rows, "Zebra",  0,  5,    -3.25f, null, null);
    
    addResultRow(rows, "apple",  0,  1001, 101.0f, null, null);
    addResultRow(rows, "beige",  0,  2000, 33.0f,  null, null);
    addResultRow(rows, "brown",  0,  1001, 53.0f,  null, null);
    addResultRow(rows, "gray", 0,  1001, 50.0f,  null, null);
    
    addResultRow(rows, "green",  0,  15,   56.0f,  null, null);
    addResultRow(rows, "hadoop", 0,  1000, 0.0f, null, null);
    addResultRow(rows, "orange", 0,  -1,   -99.0f, null, null);
    addResultRow(rows, "pig",  0,  1001, 52.0f,  null, null);
    
    addResultRow(rows, "purple", 0,  1001, 102.0f, null, null);
    addResultRow(rows, "white",  0,  1001, 100.0f, null, null);
    addResultRow(rows, "zebra",  0,  -1,   3.25f,  null, null);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a 1",	1,	null,	null,	"Reno",		"Nevada");
		addResultRow(rows, "a 2",	1,	null,	null,	"Dallas",	"Texas");
		addResultRow(rows, "a 3",	1,	null,	null,	"Phoenix",	"Arizona");
		addResultRow(rows, "a 4",	1,	null,	null,	"New York",	"New York");
		
		addResultRow(rows, "a 5",	1,	null,	null,	"Las Vegas","Nevada");
		addResultRow(rows, "a 6",	1,	null,	null,	"Santa Cruz","California");
		addResultRow(rows, "a 7",	1,	null,	null,	"San Jose","California");
		addResultRow(rows, "a 8",	1,	null,	null,	"Cupertino","California");
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records2");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(23, numbRows);
	}
	
	
	/**
	 * Compare table rows
	 * 
	 */
	private boolean compareRow(Tuple rowValues, ArrayList<Object> resultRow) throws IOException {
		boolean result = true;
		Assert.assertEquals(resultRow.size(), rowValues.size());
		for (int i = 0; i < rowValues.size(); ++i) {
			if (! compareObj(rowValues.get(i), resultRow.get(i)) ) {
				result = false;
				break;
			}
		}
		return result;
	}
	
	/**
	 * Compare table values
	 * 
	 */
	private boolean compareObj(Object object1, Object object2) {
		if (object1 == null) {
			if (object2 == null)
				return true;
			else
				return false;
		} else if (object1.equals(object2))
			return true;
		else
			return false;
	}
	
	/**
	 *Add a row to expected results table
	 * 
	 */
	private void addResultRow(ArrayList<ArrayList<Object>> resultTable, Object ... values) {
		ArrayList<Object> resultRow = new ArrayList<Object>();
		
		for (int i = 0; i < values.length; i++) {
			resultRow.add(values[i]);
		}
		resultTable.add(resultRow);
	}
	
	/**
	 * Print Pig Table (for debugging)
	 * 
	 */
	private int printTable(String tablename) throws IOException {
		Iterator<Tuple> it1 = pigServer.openIterator(tablename);
		int numbRows = 0;
		while (it1.hasNext()) {
			Tuple RowValue1 = it1.next();
			++numbRows;
			System.out.println();
			for (int i = 0; i < RowValue1.size(); ++i)
				System.out.println("DEBUG: " + tablename + " RowValue.get(" + i + ") = " + RowValue1.get(i));
		}
		System.out.println("\nRow count : " + numbRows);
		return numbRows;
	}
	
	/**
	 * Return the name of the routine that called getCurrentMethodName
	 * 
	 */
	private String getCurrentMethodName() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		(new Throwable()).printStackTrace(pw);
		pw.flush();
		String stackTrace = baos.toString();
		pw.close();
		
		StringTokenizer tok = new StringTokenizer(stackTrace, "\n");
		tok.nextToken(); // 'java.lang.Throwable'
		tok.nextToken(); // 'at ...getCurrentMethodName'
		String l = tok.nextToken(); // 'at ...<caller to getCurrentRoutine>'
		// Parse line 3
		tok = new StringTokenizer(l.trim(), " <(");
		String t = tok.nextToken(); // 'at'
		t = tok.nextToken(); // '...<caller to getCurrentRoutine>'
		return t;
	}
	
}
