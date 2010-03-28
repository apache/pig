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
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
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


public class TestOrderPreserveProjection extends BaseTestCase {
	
	final static String TABLE1_SCHEMA = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String TABLE1_STORAGE = "[a, b, c]; [d, e, f]; [m1#{a}]";
	
	final static String TABLE2_SCHEMA = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String TABLE2_STORAGE = "[a, b, c]; [d, e, f]; [m1#{a}]";
	
	final static String TABLE3_SCHEMA = "e:string,str1:string,str2:string,int1:int,map1:map(string)";
	final static String TABLE3_STORAGE = "[e, str1]; [str2,int1,map1#{a}]";
	
	final static String TABLE4_SCHEMA = "int1:int,str1:string,long1:long";
	final static String TABLE4_STORAGE = "[int1]; [str1,long1]";
	
	final static String TABLE5_SCHEMA = "b:float,d:double,c:long,e:string,f:bytes,int1:int";
	final static String TABLE5_STORAGE = "[b,d]; [c]; [e]; [f,int1]";
	
	final static String TABLE6_SCHEMA = "e:string,str3:string,str4:string";
	final static String TABLE6_STORAGE = "[e,str3,str4]";
	
	final static String TABLE7_SCHEMA = "int2:int";
	final static String TABLE7_STORAGE = "[int2]";
	
	static int fileId = 0;
	static int sortId = 0;
	
	protected static ExecJob pigJob;
	
	private static Path pathTable1;
	private static Path pathTable2;
	private static Path pathTable3;
	private static Path pathTable4;
	private static Path pathTable5;
	private static Path pathTable6;
	private static Path pathTable7;
	private static HashMap<String, String> tableStorage;
	
	private static ArrayList<String> tableList;
	
	private static Object[][] table1;
	private static Object[][] table2;
	private static Object[][] table3;
	private static Object[][] table4;
	private static Object[][] table5;
	private static Object[][] table6;
	private static Object[][] table7;
	
	private static Map<String, String> m1;
	private static Map<String, String> m2;
	private static Map<String, String> m3;
	
	@BeforeClass
	public static void setUp() throws Exception {
		init();
		pathTable1 = getTableFullPath("table1");
		pathTable2 = getTableFullPath("table2");
		pathTable3 = getTableFullPath("table3");
		pathTable4 = getTableFullPath("table4");
		pathTable5 = getTableFullPath("table5");
		pathTable6 = getTableFullPath("table6");
		pathTable7 = getTableFullPath("table7");
		
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
		m3 = new HashMap<String, String>();
		m3.put("a","m3-a");
		m3.put("b","m3-b");
		m3.put("c","m3-b");
		
		table3 = new Object[][] {
			{"a 8",	"Cupertino",	"California",	1,	m3},
			{"a 7",	"San Jose",		"California",	2,	m3},
			{"a 6",	"Santa Cruz",	"California",	3,	m3},
			{"a 5",	"Las Vegas",	"Nevada",		4,	m3},
			{"a 4",	"New York",		"New York",		5,	m3},
			{"a 3",	"Phoenix",		"Arizona",		6,	m3},
			{"a 2",	"Dallas",		"Texas",		7,	m3},
			{"a 1",	"Reno",			"Nevada",		8,	m3} };
		
		// Create table3
		createTable(pathTable3, TABLE3_SCHEMA, TABLE3_STORAGE, table3);
		
		// Create table4 data
		table4 = new Object[][] {
			{1,	"Cupertino",	1001L},
			{2,	"San Jose",		1008L},
			{3,	"Santa Cruz",	1008L},
			{4,	"Las Vegas",	1008L},
			{5,	"Dallas",		1010L},
			{6,	"Reno",			1000L} };
		
		// Create table4
		createTable(pathTable4, TABLE4_SCHEMA, TABLE4_STORAGE, table4);
		
		// Create table5 data
		table5 = new Object[][] {
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100},
			{3.25f,		50e+2,	1000L,	"string1",	new DataByteArray("blue"),	100},
			{3.26f,		51e+2,	1001L,	"string0",	new DataByteArray("purple"),100},
			{3.24f,		53e+2,	1001L,	"string1",	new DataByteArray("yellow"),100},
			{3.28f,		51e+2,	1001L,	"string2",	new DataByteArray("white"),	100},
			{3.25f,		53e+2,	1000L,	"string0",	new DataByteArray("orange"),100} };
		
		// Create table5
		createTable(pathTable5, TABLE5_SCHEMA, TABLE5_STORAGE, table5);
		
		// Create table6 data
		table6 = new Object[][] {
			{"a 8",	"Cupertino",	"California"},
			{"a 7",	"San Jose",		"California"},
			{"a 6",	"Santa Cruz",	"California"},
			{"a 5",	"Las Vegas",	"Nevada"},
			{"a 4",	"New York",		"New York"},
			{"a 3",	"Phoenix",		"Arizona"},
			{"a 2",	"Dallas",		"Texas"},
			{"a 1",	"Reno",			"Nevada"} };
		
		// Create table6
		createTable(pathTable6, TABLE6_SCHEMA, TABLE6_STORAGE, table6);
		
		// Create table7 data
		table7 = new Object[][] {
			{1001},
			{1000},
			{1010},
			{1009},
			{1001} };
		
		// Create table7
		createTable(pathTable7, TABLE7_SCHEMA, TABLE7_STORAGE, table7);
		
		// Load tables
		String query1 = "table1 = LOAD '" + pathTable1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query1);
		String query2 = "table2 = LOAD '" + pathTable2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query2);
		String query3 = "table3 = LOAD '" + pathTable3.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query3);
		String query4 = "table4 = LOAD '" + pathTable4.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query4);
		String query5 = "table5 = LOAD '" + pathTable5.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query5);
		String query6 = "table6 = LOAD '" + pathTable6.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query6);
		String query7 = "table7 = LOAD '" + pathTable7.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query7);
		
		// Map for table storage
		tableStorage = new HashMap<String, String>();
		tableStorage.put("table1", TABLE1_STORAGE);
		tableStorage.put("table2", TABLE2_STORAGE);
		tableStorage.put("table3", TABLE3_STORAGE);
		tableStorage.put("table4", TABLE4_STORAGE);
		tableStorage.put("table5", TABLE5_STORAGE);
		tableStorage.put("table6", TABLE6_STORAGE);
		tableStorage.put("table7", TABLE7_STORAGE);
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
	
	private Iterator<Tuple> testOrderPreserveUnion(Map<String, String> inputTables, String columns) throws IOException {
		//
		// Test order preserve union from input tables and provided output columns
		//
		Assert.assertTrue("Table union requires two or more input tables", inputTables.size() >= 2);
		
		Path newPath = new Path(getCurrentMethodName());
		ArrayList<String> pathList = new ArrayList<String>();
		tableList = new ArrayList<String>(); // list of tables to load
		
		// Load and store each of the input tables
		for (Iterator<String> it = inputTables.keySet().iterator(); it.hasNext();) {
			
			String tablename = it.next();
			String sortkey = inputTables.get(tablename);
			
			String sortName = "sort" + ++sortId;
			
			// Sort tables
			String orderby = sortName + " = ORDER " + tablename + " BY " + sortkey + " ;";   // need single quotes?
			pigServer.registerQuery(orderby);
			
			String sortPath = new String(newPath.toString() + ++fileId);  // increment fileId suffix
			
			// Store sorted tables
			pigJob = pigServer.store(sortName, sortPath, TableStorer.class.getCanonicalName() +
				"('" + tableStorage.get(tablename) + "')");
			Assert.assertNull(pigJob.getException());
			
			tableList.add(tablename); // add table name to list
			pathList.add(sortPath);  // add table path to list
		}
		
		String paths = new String();
		for (String path:pathList)
			paths += path + ",";
		paths = paths.substring(0, paths.lastIndexOf(","));  // remove trailing comma
		
		String queryLoad = "records1 = LOAD '"
	        + paths
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('" + columns + "', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Return iterator
		Iterator<Tuple> it1 = pigServer.openIterator("records1");
		return it1;
	}
	
	
	@Test
	public void test_sorted_table_union_01() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a,b,c");  // input table and sort keys
		inputTables.put("table2", "a,b,c");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "a,b,c,d,e,f,m1, source_table");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, -1, 3.25f,  1000L,  50e+2,  "zebra",  new DataByteArray("zebra"), m1);
    addResultRow(rows, 5,  -3.25f, 1001L,  51e+2,  "Zebra",  new DataByteArray("Zebra"), m1);
    addResultRow(rows, 1000, 0.0f, 1002L,  52e+2,  "hadoop", new DataByteArray("hadoop"),m1);
    addResultRow(rows, 1001, 50.0f,  1000L,  50e+2,  "Pig",    new DataByteArray("Pig"), m1);
    addResultRow(rows, 1001, 52.0f,  1001L,  50e+2,  "pig",    new DataByteArray("pig"), m1);
    addResultRow(rows, 1001, 100.0f, 1003L,  50e+2,  "Apple",  new DataByteArray("Apple"), m1);
    addResultRow(rows, 1001, 101.0f, 1001L,  50e+2,  "apple",  new DataByteArray("apple"), m1);
    addResultRow(rows, 1002, 28.0f,  1000L,  50e+2,  "Hadoop", new DataByteArray("Hadoop"),m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, -1, -99.0f, 1002L,  51e+2,  "orange", new DataByteArray("orange"),m2);
    addResultRow(rows, 15, 56.0f,  1004L,  50e+2,  "green",  new DataByteArray("green"), m2);
    addResultRow(rows, 1001, 50.0f,  1008L,  52e+2,  "gray",   new DataByteArray("gray"),  m2);
    addResultRow(rows, 1001, 53.0f,  1001L,  52e+2,  "brown",  new DataByteArray("brown"), m2);
    addResultRow(rows, 1001, 100.0f, 1003L,  55e+2,  "white",  new DataByteArray("white"), m2);
    addResultRow(rows, 1001, 102.0f, 1001L,  52e+2,  "purple", new DataByteArray("purple"),m2);
    addResultRow(rows, 2000, 33.0f,  1006L,  52e+2,  "beige",  new DataByteArray("beige"), m2);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 7, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_sorted_table_union_02() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a,b,c");  // input table and sort keys
		inputTables.put("table2", "a,b,c");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "a,b,c,d,e,f,m1,source_table");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, -1,	3.25f,	1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),	m1,	0);
		addResultRow(rows, 5,	-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),	m1,	0);
		addResultRow(rows, 1000,	0.0f,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),m1,	0);
		addResultRow(rows, 1001,	50.0f,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),	m1,	0);
		addResultRow(rows, 1001,	52.0f,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),	m1,	0);
		addResultRow(rows, 1001,	100.0f,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),	m1,	0);
		addResultRow(rows, 1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1,	0);
		addResultRow(rows, 1002,	28.0f,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),m1,	0);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, -1,	-99.0f,	1002L,	51e+2,	"orange",	new DataByteArray("orange"),m2,	1);
		addResultRow(rows, 15, 56.0f,  1004L,  50e+2,  "green",  new DataByteArray("green"), m2, 1);
		addResultRow(rows, 1001, 50.0f,  1008L,  52e+2,  "gray",   new DataByteArray("gray"),  m2, 1);
		addResultRow(rows, 1001, 53.0f,  1001L,  52e+2,  "brown",  new DataByteArray("brown"), m2, 1);
		addResultRow(rows, 1001, 100.0f, 1003L,  55e+2,  "white",  new DataByteArray("white"), m2, 1);
		addResultRow(rows, 1001, 102.0f, 1001L,  52e+2,  "purple", new DataByteArray("purple"),m2, 1);
		addResultRow(rows, 2000, 33.0f,  1006L,  52e+2,  "beige",  new DataByteArray("beige"), m2, 1);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 7, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	/**
	 * This is a negative test case, as we don't support partial sort key match.
	 * 
	 * @throws ExecException
	 * @throws IOException
	 */
	@Test
	public void test_sorted_table_union_03() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a,b,c");  // input table and sort keys
		inputTables.put("table2", "a,b");  // input table and sort keys
		
		IOException exception = null;
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "a,b,c,d,e,f,m1,source_table");
		} catch(IOException ex) {
			exception = ex;
		} finally {
			Assert.assertTrue( exception != null );
			Assert.assertTrue( getStackTrace( exception ).contains( "Tables of the table union do not possess the same sort property" ) );
		}
		
//		// Verify union table
//		ArrayList<ArrayList<Object>> resultTable = new ArrayList<ArrayList<Object>>();
//		
//		addResultRow(resultTable, -1,	-99.0f,	1002L,	51e+2,	"orange",	new DataByteArray("orange"),m2,	1);
//		addResultRow(resultTable, -1,	3.25f,	1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),	m1,	0);
//		addResultRow(resultTable, 5,	-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),	m1,	0);
//		addResultRow(resultTable, 15,	56.0f,	1004L,	50e+2,	"green",	new DataByteArray("green"),	m2,	1);
//		
//		addResultRow(resultTable, 1000,	0.0f,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),m1,	0);
//		addResultRow(resultTable, 1001,	50.0f,	1008L,	52e+2,	"gray",		new DataByteArray("gray"),	m2,	1);
//		addResultRow(resultTable, 1001,	50.0f,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),	m1,	0);
//		addResultRow(resultTable, 1001,	52.0f,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),	m1,	0);
//		
//		addResultRow(resultTable, 1001,	53.0f,	1001L,	52e+2,	"brown",	new DataByteArray("brown"),	m2,	1);
//		addResultRow(resultTable, 1001,	100.0f,	1003L,	55e+2,	"white",	new DataByteArray("white"),	m2,	1);
//		addResultRow(resultTable, 1001,	100.0f,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),	m1,	0);
//		addResultRow(resultTable, 1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1,	0);
//		
//		addResultRow(resultTable, 1001,	102.0f,	1001L,	52e+2,	"purple",	new DataByteArray("purple"),m2,	1);
//		addResultRow(resultTable, 1002,	28.0f,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),m1,	0);
//		addResultRow(resultTable, 2000,	33.0f,	1006L,	52e+2,	"beige",	new DataByteArray("beige"),	m2,	1);
//		
//		// Verify union table
//		Iterator<Tuple> it = pigServer.openIterator("records1");
//		int numbRows = verifyTable(resultTable, 0, it);
//		
//		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	/**
	 * Get stack trace as string that includes nested exceptions
	 * 
	 */
	private static String getStackTrace(Throwable throwable) {
		Writer writer = new StringWriter();
		PrintWriter printWriter = new PrintWriter(writer);
		if (throwable != null)
			throwable.printStackTrace(printWriter);
		return writer.toString();
	}

	@Test
	public void test_sorted_table_union_04() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "source_table,e,c,b,a");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,	"Apple",	1003L,	100.0f,	1001);
		addResultRow(rows, 0,	"Hadoop",	1000L,	28.0f,	1002);
		addResultRow(rows, 0,	"Pig",		1000L,	50.0f,	1001);
		addResultRow(rows, 0,	"Zebra",	1001L,	-3.25f,	5);
		
		addResultRow(rows, 0,	"apple",	1001L,	101.0f,	1001);
		addResultRow(rows, 0,	"hadoop",	1002L,	0.0f,	1000);
		addResultRow(rows, 0,	"pig",		1001L,	52.0f,	1001);
		addResultRow(rows, 0,	"zebra",	1000L,	3.25f,	-1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,  "beige",  1006L,  33.0f,  2000);
    addResultRow(rows, 1,  "brown",  1001L,  53.0f,  1001);
    addResultRow(rows, 1,  "gray",   1008L,  50.0f,  1001);
    addResultRow(rows, 1,  "green",  1004L,  56.0f,  15);
    addResultRow(rows, 1,  "orange", 1002L,  -99.0f, -1);
    addResultRow(rows, 1,  "purple", 1001L,  102.0f, 1001);
    addResultRow(rows, 1,  "white",  1003L,  100.0f, 1001);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	
	@Test
	public void test_sorted_table_union_05() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e,c");  // input table and sort keys
		inputTables.put("table2", "e,c");  // input table and sort keys
		inputTables.put("table5", "e,c");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "e,c,a,b,d,f,m1,source_table");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",	1003L,	1001,	100.0f,	50e+2,	new DataByteArray("Apple"),	m1,		0);
		addResultRow(rows, "Hadoop",	1000L,	1002,	28.0f,	50e+2,	new DataByteArray("Hadoop"),m1,		0);
		addResultRow(rows, "Pig",	1000L,	1001,	50.0f,	50e+2,	new DataByteArray("Pig"),	m1,		0);
		addResultRow(rows, "Zebra",	1001L,	5,		-3.25f,	51e+2,	new DataByteArray("Zebra"),	m1,		0);
		addResultRow(rows, "apple",	1001L,	1001,	101.0f,	50e+2,	new DataByteArray("apple"),	m1,		0);
		addResultRow(rows, "hadoop",	1002L,	1000,	0.0f,	52e+2,	new DataByteArray("hadoop"),m1,		0);
		addResultRow(rows, "pig",	1001L,	1001,	52.0f,	50e+2,	new DataByteArray("pig"),	m1,		0);
		addResultRow(rows, "zebra",	1000L,	-1,		3.25f,	50e+2,	new DataByteArray("zebra"),	m1,		0);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "beige",  1006L,  2000, 33.0f,  52e+2,  new DataByteArray("beige"), m2,   1);
    addResultRow(rows, "brown",  1001L,  1001, 53.0f,  52e+2,  new DataByteArray("brown"), m2,   1);
    addResultRow(rows, "gray", 1008L,  1001, 50.0f,  52e+2,  new DataByteArray("gray"),  m2,   1);
    addResultRow(rows, "green",  1004L,  15,   56.0f,  50e+2,  new DataByteArray("green"), m2,   1);
    addResultRow(rows, "orange", 1002L,  -1,   -99.0f, 51e+2,  new DataByteArray("orange"),m2,   1);
    addResultRow(rows, "purple", 1001L,  1001, 102.0f, 52e+2,  new DataByteArray("purple"),m2,   1);
    addResultRow(rows, "white",  1003L,  1001, 100.0f, 55e+2,  new DataByteArray("white"), m2,   1);
		resultTable.put(1, rows);
    
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "string0",1000L,  null, 3.25f,  53e+2,  new DataByteArray("orange"),null, 2);
    addResultRow(rows, "string0",1001L,  null, 3.26f,  51e+2,  new DataByteArray("purple"),null, 2);
    addResultRow(rows, "string1",1000L,  null, 3.25f,  50e+2,  new DataByteArray("blue"),  null, 2);
    addResultRow(rows, "string1",1001L,  null, 3.25f,  51e+2,  new DataByteArray("green"), null, 2);
    addResultRow(rows, "string1",1001L,  null, 3.24f,  53e+2,  new DataByteArray("yellow"),null, 2);
    addResultRow(rows, "string2",1001L,  null, 3.28f,  51e+2,  new DataByteArray("white"), null, 2);
		resultTable.put(2, rows);
    
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 7, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length + table5.length);
	}
	
	@Test
	public void test_sorted_table_union_06() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table5", "e");  // input table and sort keys
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "b,a,source_table,c,d,f,m1,e");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows,	100.0f,	1001,	0,	1003L,	50e+2,	new DataByteArray("Apple"),	m1,	"Apple");
		addResultRow(rows,	28.0f,	1002,	0,	1000L,	50e+2,	new DataByteArray("Hadoop"),m1,	"Hadoop");
		addResultRow(rows,	50.0f,	1001,	0,	1000L,	50e+2,	new DataByteArray("Pig"),	m1,	"Pig");
		addResultRow(rows,	-3.25f,	5,		0,	1001L,	51e+2,	new DataByteArray("Zebra"),	m1,	"Zebra");
		addResultRow(rows,	101.0f,	1001,	0,	1001L,	50e+2,	new DataByteArray("apple"),	m1,	"apple");
		addResultRow(rows,	0.0f,	1000,	0,	1002L,	52e+2,	new DataByteArray("hadoop"),m1,	"hadoop");
		addResultRow(rows,	52.0f,	1001,	0,	1001L,	50e+2,	new DataByteArray("pig"),	m1,	"pig");
		addResultRow(rows,	3.25f,	-1,		0,	1000L,	50e+2,	new DataByteArray("zebra"),	m1,	"zebra");
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, 33.0f,  2000, 1,  1006L,  52e+2,  new DataByteArray("beige"), m2, "beige");
    addResultRow(rows, 53.0f,  1001, 1,  1001L,  52e+2,  new DataByteArray("brown"), m2, "brown");
    addResultRow(rows, 50.0f,  1001, 1,  1008L,  52e+2,  new DataByteArray("gray"),  m2, "gray");
    addResultRow(rows, 56.0f,  15,   1,  1004L,  50e+2,  new DataByteArray("green"), m2, "green");
    addResultRow(rows, -99.0f, -1,   1,  1002L,  51e+2,  new DataByteArray("orange"),m2, "orange");
    addResultRow(rows, 102.0f, 1001, 1,  1001L,  52e+2,  new DataByteArray("purple"),m2, "purple");
    addResultRow(rows, 100.0f, 1001, 1,  1003L,  55e+2,  new DataByteArray("white"), m2, "white");
		resultTable.put(1, rows);
    
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 3.26f,  null, 2,  1001L,  51e+2,  new DataByteArray("purple"),null,"string0");
    addResultRow(rows, 3.25f,  null, 2,  1000L,  53e+2,  new DataByteArray("orange"),null,"string0");
    addResultRow(rows, 3.25f,  null, 2,  1001L,  51e+2,  new DataByteArray("green"), null,"string1");
    
    addResultRow(rows, 3.25f,  null, 2,  1000L,  50e+2,  new DataByteArray("blue"),  null,"string1");
    addResultRow(rows, 3.24f,  null, 2,  1001L,  53e+2,  new DataByteArray("yellow"),null,"string1");
    addResultRow(rows, 3.28f,  null, 2,  1001L,  51e+2,  new DataByteArray("white"), null,"string2");
		resultTable.put(2, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 7, 2, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length + table5.length);
	}
	
	/**
	 * Disable this test case as it also belongs to a negative test case, which is already covered in test case 03.
	 * @throws ExecException
	 * @throws IOException
	 */
	//@Test
	public void test_sorted_table_union_07() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e,b,c");  // input table and sort keys
		inputTables.put("table2", "e,b");  // input table and sort keys
		inputTables.put("table5", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "e,source_table,b,c,int1,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",	0,	100.0f,	1003L,	null,	new DataByteArray("Apple"),	m1);
		addResultRow(rows, "Hadoop",	0,	28.0f,	1000L,	null,	new DataByteArray("Hadoop"),m1);
		addResultRow(rows, "Pig",	0,	50.0f,	1000L,	null,	new DataByteArray("Pig"),	m1);
		addResultRow(rows, "Zebra",	0,	-3.25f,	1001L,	null,	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, "apple",	0,	101.0f,	1001L,	null,	new DataByteArray("apple"),	m1);
		addResultRow(rows, "hadoop",	0,	0.0f,	1002L,	null,	new DataByteArray("hadoop"),m1);
		addResultRow(rows, "pig",	0,	52.0f,	1001L,	null,	new DataByteArray("pig"),	m1);
		addResultRow(rows, "zebra",	0,	3.25f,	1000L,	null,	new DataByteArray("zebra"),	m1);
		resultTable.put(0, rows);
		
		addResultRow(rows, "beige",  1,  33.0f,  1006L,  null, new DataByteArray("beige"), m2);
    addResultRow(rows, "brown",  1,  53.0f,  1001L,  null, new DataByteArray("brown"), m2);
    addResultRow(rows, "gray", 1,  50.0f,  1008L,  null, new DataByteArray("gray"),  m2);
    addResultRow(rows, "green",  1,  56.0f,  1004L,  null, new DataByteArray("green"), m2);
    addResultRow(rows, "orange", 1,  -99.0f, 1002L,  null, new DataByteArray("orange"),m2);
    addResultRow(rows, "purple", 1,  102.0f, 1001L,  null, new DataByteArray("purple"),m2);
    addResultRow(rows, "white",  1,  100.0f, 1003L,  null, new DataByteArray("white"), m2);
		resultTable.put(1, rows);
		
		addResultRow(rows, "string0",2,  3.26f,  1001L,  100,  new DataByteArray("purple"),null);
    addResultRow(rows, "string0",2,  3.25f,  1000L,  100,  new DataByteArray("orange"),null);
    addResultRow(rows, "string1",2,  3.25f,  1001L,  100,  new DataByteArray("green"),null);
    addResultRow(rows, "string1",2,  3.25f,  1000L,  100,  new DataByteArray("blue"),null);
    addResultRow(rows, "string1",2,  3.24f,  1001L,  100,  new DataByteArray("yellow"),null);
    addResultRow(rows, "string2",2,  3.28f,  1001L,  100,  new DataByteArray("white"),null);
		resultTable.put(2, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length + table5.length);
	}
	
	@Test
	public void test_sorted_table_union_08() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		inputTables.put("table3", "e");  // input table and sort keys
		inputTables.put("table5", "e");  // input table and sort keys
		inputTables.put("table6", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "e,source_table,a,b,c,m1,int1,str1,str2,str3,str4,map1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, "Apple",  0,  1001, 100.0f, 1003L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "Hadoop", 0,  1002, 28.0f,  1000L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "Pig",  0,  1001, 50.0f,  1000L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "Zebra",  0,  5,    -3.25f, 1001L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "apple",  0,  1001, 101.0f, 1001L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "hadoop", 0,  1000, 0.0f, 1002L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "pig",  0,  1001, 52.0f,  1001L,  m1,   null, null, null, null, null, null);
    addResultRow(rows, "zebra",  0,  -1,   3.25f,  1000L,  m1,   null, null, null, null, null, null);
		resultTable.put(0, rows);
    
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a 1",  1,  null, null, null, null, 8,    "Reno",   "Nevada", null,   null,         m3);
    addResultRow(rows, "a 2",  1,  null, null, null, null, 7,    "Dallas", "Texas",  null,   null,         m3);
    addResultRow(rows, "a 3",  1,  null, null, null, null, 6,    "Phoenix",  "Arizona",  null,   null,         m3);
    addResultRow(rows, "a 4",  1,  null, null, null, null, 5,    "New York", "New York", null,   null,         m3);
    addResultRow(rows, "a 5",  1,  null, null, null, null, 4,    "Las Vegas","Nevada", null,   null,         m3);
    addResultRow(rows, "a 6",  1,  null, null, null, null, 3,    "Santa Cruz","California",  null, null,         m3);
    addResultRow(rows, "a 7",  1,  null, null, null, null, 2,    "San Jose","California",null,   null,         m3);
    addResultRow(rows, "a 8",  1,  null, null, null, null, 1,    "Cupertino","California",null,  null,         m3);
		resultTable.put(1, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, "beige",  2,  2000, 33.0f,  1006L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "brown",  2,  1001, 53.0f,  1001L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "gray", 2,  1001, 50.0f,  1008L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "green",  2,  15,   56.0f,  1004L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "orange", 2,  -1,   -99.0f, 1002L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "purple", 2,  1001, 102.0f, 1001L,  m2,   null, null, null, null, null, null);
    addResultRow(rows, "white",  2,  1001, 100.0f, 1003L,  m2,   null, null, null, null, null, null);
		resultTable.put(2, rows);
    
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "string0",3,  null, 3.26f,  1001L,  null, 100,  null, null, null, null, null);
    addResultRow(rows, "string0",3,  null, 3.25f,  1000L,  null, 100,  null, null, null, null, null);
    addResultRow(rows, "string1",3,  null, 3.25f,  1001L,  null, 100,  null, null, null, null, null);
    addResultRow(rows, "string1",3,  null, 3.25f,  1000L,  null, 100,  null, null, null, null, null);
    addResultRow(rows, "string1",3,  null, 3.24f,  1001L,  null, 100,  null, null, null, null, null);
    addResultRow(rows, "string2",3,  null, 3.28f,  1001L,  null, 100,  null, null, null, null, null);
		resultTable.put(3, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a 1",  4,  null, null, null, null, null, null,   null,   "Reno",   "Nevada", null);
		addResultRow(rows, "a 2",  4,  null, null, null, null, null, null,   null,   "Dallas", "Texas",    null);
		addResultRow(rows, "a 3",  4,  null, null, null, null, null, null,   null,   "Phoenix",  "Arizona",  null);
		addResultRow(rows, "a 4",  4,  null, null, null, null, null, null,   null,   "New York", "New York",   null);
		addResultRow(rows, "a 5",  4,  null, null, null, null, null, null,   null,   "Las Vegas","Nevada", null);
		addResultRow(rows, "a 6",  4,  null, null, null, null, null, null,   null,   "Santa Cruz","California",    null);
		addResultRow(rows, "a 7",  4,  null, null, null, null, null, null,   null,   "San Jose","California",  null);
		addResultRow(rows, "a 8",  4,  null, null, null, null, null, null,   null,   "Cupertino","California",   null);
		resultTable.put(4, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length + table3.length +
				table5.length + table6.length);
	}
	
	@Test
	public void test_sorted_table_union_09() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "e,source_table,xx,yy,zz"); // extra undefined columns
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "Apple",	0,	null, null, null);
		addResultRow(rows, "Hadoop",	0,	null, null, null);
		addResultRow(rows, "Pig",	0,	null, null, null);
		addResultRow(rows, "Zebra",	0,	null, null, null);
		addResultRow(rows, "apple",	0,	null, null, null);
		addResultRow(rows, "hadoop",	0,	null, null, null);
		addResultRow(rows, "pig",	0,	null, null, null);
		addResultRow(rows, "zebra",	0,	null, null, null);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "beige",  1,  null, null, null);
    addResultRow(rows, "brown",  1,  null, null, null);
    addResultRow(rows, "gray", 1,  null, null, null);
    addResultRow(rows, "green",  1,  null, null, null);
    addResultRow(rows, "orange", 1,  null, null, null);
    addResultRow(rows, "purple", 1,  null, null, null);
    addResultRow(rows, "white",  1,  null, null, null);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 1, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_sorted_table_union_10() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a,b");  // input table and sort keys
		inputTables.put("table2", "a,b");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "source_table, a,  b,  c  ,,,  d ,  e,,,"); // extra commas and spaces
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0, -1,	3.25f,	1000L,	null, null,	50e+2,	"zebra");
		addResultRow(rows, 0, 5,	-3.25f,	1001L,	null, null,	51e+2,	"Zebra");
		addResultRow(rows, 0, 1000,	0.0f,	1002L,	null, null,	52e+2,	"hadoop");
		addResultRow(rows, 0, 1001,	50.0f,	1000L,	null, null,	50e+2,	"Pig");
		addResultRow(rows, 0, 1001,	52.0f,	1001L,	null, null,	50e+2,	"pig");
		addResultRow(rows, 0, 1001,	100.0f,	1003L,	null, null,	50e+2,	"Apple");
		addResultRow(rows, 0, 1001,	101.0f,	1001L,	null, null,	50e+2,	"apple");
		addResultRow(rows, 0, 1002,	28.0f,	1000L,	null, null,	50e+2,	"Hadoop");
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1, -1,	-99.0f,	1002L,	null, null,	51e+2,	"orange");
		addResultRow(rows, 1, 15, 56.0f,  1004L,  null, null, 50e+2,  "green");
		addResultRow(rows, 1, 1001, 50.0f,  1008L,  null, null, 52e+2,  "gray");
		addResultRow(rows, 1, 1001, 53.0f,  1001L,  null, null, 52e+2,  "brown");
		addResultRow(rows, 1, 1001, 100.0f, 1003L,  null, null, 55e+2,  "white");
		addResultRow(rows, 1, 1001, 102.0f, 1001L,  null, null, 52e+2,  "purple");
		addResultRow(rows, 1, 2000, 33.0f,  1006L,  null, null, 52e+2,  "beige");
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_sorted_table_union_11() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "source_table,b,c,int1,f,m1");  // sort key e not in projection
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,	100.0f,	1003L,	null,	new DataByteArray("Apple"),	m1);
		addResultRow(rows, 0,	28.0f,	1000L,	null,	new DataByteArray("Hadoop"),m1);
		addResultRow(rows, 0,	50.0f,	1000L,	null,	new DataByteArray("Pig"),	m1);
		addResultRow(rows, 0,	-3.25f,	1001L,	null,	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, 0,	101.0f,	1001L,	null,	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	0.0f,	1002L,	null,	new DataByteArray("hadoop"),m1);
		addResultRow(rows, 0,	52.0f,	1001L,	null,	new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,	3.25f,	1000L,	null,	new DataByteArray("zebra"),	m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,  33.0f,  1006L,  null, new DataByteArray("beige"), m2);
    addResultRow(rows, 1,  53.0f,  1001L,  null, new DataByteArray("brown"), m2);
    addResultRow(rows, 1,  50.0f,  1008L,  null, new DataByteArray("gray"),  m2);
    addResultRow(rows, 1,  56.0f,  1004L,  null, new DataByteArray("green"), m2);
    addResultRow(rows, 1,  -99.0f, 1002L,  null, new DataByteArray("orange"),m2);
    addResultRow(rows, 1,  102.0f, 1001L,  null, new DataByteArray("purple"),m2);
    addResultRow(rows, 1,  100.0f, 1003L,  null, new DataByteArray("white"), m2);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 4, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
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
	 * Print loaded tables with indexes (for debugging)
	 * 
	 */
	private void printTableList() {
		System.out.println();
		for (int i=0; i<tableList.size(); ++i) {
			System.out.println("Load table  " + i + " : " + tableList.get(i));
		}
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
