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


public class TestOrderPreserveProjectionNegative extends BaseTestCase {
	
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
	
	final static String TABLE6_SCHEMA = "a:string,b:string,str1:string";
	final static String TABLE6_STORAGE = "[a,b,str1]";
	
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
	
	private static Object[][] table1;
	private static Object[][] table2;
	private static Object[][] table3;
	private static Object[][] table4;
	private static Object[][] table5;
	private static Object[][] table6;
	private static Object[][] table7;

  @BeforeClass
  public static void setUp() throws Exception {
      init();
      pathTable1 = getTableFullPath("TestOrderPerserveProjectionNegative1");
      pathTable2 = getTableFullPath("TestOrderPerserveProjectionNegative2");
      pathTable3 = getTableFullPath("TestOrderPerserveProjectionNegative3");
      pathTable4 = getTableFullPath("TestOrderPerserveProjectionNegative4");
      pathTable5 = getTableFullPath("TestOrderPerserveProjectionNegative5");
      pathTable6 = getTableFullPath("TestOrderPerserveProjectionNegative6");
      pathTable7 = getTableFullPath("TestOrderPerserveProjectionNegative7");
      
      removeDir(pathTable1);
      removeDir(pathTable2);
      removeDir(pathTable3);
      removeDir(pathTable4);
      removeDir(pathTable5);
      removeDir(pathTable6);
      removeDir(pathTable7);

		// Create table1 data
		Map<String, String> m1 = new HashMap<String, String>();
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
		Map<String, String> m2 = new HashMap<String, String>();
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
		Map<String, String> m3 = new HashMap<String, String>();
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
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100},
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100},
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100},
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100},
			{3.25f,		51e+2,	1001L,	"string1",	new DataByteArray("green"),	100} };
		
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
		String query8 = "table8 = LOAD '" + pathTable7.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query8);
		
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
	public void union_error_exist_source_table() throws ExecException {
		//
		// Test that we do not allow user to create table with column "source_table" (Negative test)
		//
		IOException exception = null;
		
		String STR_SCHEMA_TEST = "a:string,b:string,source_table:int";
		String STR_STORAGE_TEST = "[a, b]; [source_table]";
		
		// Create table1 data
		Object[][] table_test = new Object[][] {
			{"a1",	"z",	5},
			{"a2",	"r",	4},
			{"a3",	"e",	3},
			{"a4",	"a",	1} };
		
//		Path pathTable1 = new Path(pathWorking, "table_test");
		
		try {
			// Create table1
			createTable(pathTable1, STR_SCHEMA_TEST, STR_STORAGE_TEST, table_test);
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(exception.toString().contains("[source_table] is a reserved virtual column name"));
		}
	}
	
	@Test
	public void union_error_diff_type() throws ExecException {
		//
		// Test sorted union error handling when tables have same sort key of different types (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a");  // input table and sort keys
		inputTables.put("table6", "a");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "a,b,c,d,e,f,m1");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("Different types of column"));
		}
	}
	
	@Test
	public void union_error_complex_type() throws ExecException {
		//
		// Test sorted union error handling when tables have complex map sort keys (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "m1");  // input table and sort keys
		inputTables.put("table2", "m1");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "a,b,c,d,e,f,m1");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("not of simple type as required for a sort column now"));
		}
	}
	
	@Test
	public void union_error_invalid_path1() throws ExecException {
		//
		// Test sorted union error handling when one of the table paths is invalid (Negative test)
		//
		IOException exception = null;
		
		try {
			// Sort tables
			String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
			pigServer.registerQuery(orderby1);
			
			Path newPath = new Path(getCurrentMethodName());
			
			// Store sorted tables
			String pathSort1 = newPath.toString() + "1";
			pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
				"('" + TABLE1_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String pathSort2 = newPath.toString() + "2";  // invalid path
			
			String queryLoad = "records1 = LOAD '"
		        + pathSort1 + ","
		        + pathSort2
		        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c', 'sorted');";
			pigServer.registerQuery(queryLoad);
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
                        Assert.assertTrue(getStackTrace(exception).contains("Input path does not exist: "));
		}
	}
	
	@Test
	public void union_error_invalid_path2() throws ExecException {
		//
		// Test sorted union error handling when one of the table paths is invalid (Negative test)
		//
		IOException exception = null;
		String pathSort2 = null;
		
		try {
			// Sort tables
			String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
			pigServer.registerQuery(orderby1);
			
			Path newPath = new Path(getCurrentMethodName());
			
			// Store sorted tables
			String pathSort1 = newPath.toString() + "1";
			pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
				"('" + TABLE1_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			pathSort2 = newPath.toString() + "2";  // invalid path
			
			String queryLoad = "records1 = LOAD '"
		        + pathSort1 + ","
		        + pathSort2
		        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c,source_table', 'sorted');";
			pigServer.registerQuery(queryLoad);
			
			printTable("records1");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			System.out.println(getStackTrace(exception));
			//Assert.assertNotNull(exception);
			//Assert.assertTrue(getStackTrace(exception).contains("Schema file doesn't exist"));
		}
	}
	
	@Test
	public void union_error_invalid_column() throws ExecException {
		//
		// Test sorted union error handling when some of the output columns are not in any
		// of the tables (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "e,source_table,a,xx,yy,zz");
			
			printTable("records1");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			System.out.println(getStackTrace(exception));
			//Assert.assertNotNull(exception);
			//Assert.assertTrue(getStackTrace(exception).contains("Using Map as key not supported"));
		}
	}
	
	@Test
	public void union_error_invalid_key() throws ExecException {
		//
		// Test sorted union error handling where column sort keys are missing from
		// projection (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "e");  // input table and sort keys
		inputTables.put("table2", "e");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "a");
			
			printTable("records1");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			System.out.println(getStackTrace(exception));
			//Assert.assertNotNull(exception);
			//Assert.assertTrue(getStackTrace(exception).contains("Using Map as key not supported"));
		}
	}
	
	@Test
	public void union_error_unsorted_left() throws ExecException {
		//
		// Test sorted union error handling where left table is not sorted (Negative test)
		//
		IOException exception = null;
		
		try {
			// Sort tables
			String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
			pigServer.registerQuery(orderby2);
			
			Path newPath = new Path(getCurrentMethodName());
			
			// Store sorted tables
			String pathSort1 = newPath.toString() + "1";
			pigJob = pigServer.store("table1", pathSort1, TableStorer.class.getCanonicalName() +
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
			pigServer.registerQuery(queryLoad);
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("The table is not sorted"));
		}
	}
	
	@Test
	public void union_error_unsorted_right() throws ExecException {
		//
		// Test sorted union error handling where right table is not sorted (Negative test)
		//
		IOException exception = null;
		
		try {
			// Sort tables
			String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
			pigServer.registerQuery(orderby1);
			
			Path newPath = new Path(getCurrentMethodName());
			
			// Store sorted tables
			String pathSort1 = newPath.toString() + "1";
			pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
				"('" + TABLE1_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String pathSort2 = newPath.toString() + "2";
			pigJob = pigServer.store("table2", pathSort2, TableStorer.class.getCanonicalName() +
				"('" + TABLE2_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String queryLoad = "records1 = LOAD '"
		        + pathSort1 + ","
		        + pathSort2
		        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,source_table,b,c,d,e,f,m1', 'sorted');";
			pigServer.registerQuery(queryLoad);
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("The table is not sorted"));
		}
	}
	
	@Test
	public void union_error_unsorted_middle() throws ExecException {
		//
		// Test sorted union error handling where middle table is not sorted (Negative test)
		//
		IOException exception = null;
		
		try {
			// Sort tables
			String orderby1 = "sort1 = ORDER table1 BY " + "e" + " ;";
			pigServer.registerQuery(orderby1);
			
			String orderby3 = "sort3 = ORDER table3 BY " + "e" + " ;";
			pigServer.registerQuery(orderby3);
			
			Path newPath = new Path(getCurrentMethodName());
			
			// Store sorted tables
			String pathSort1 = newPath.toString() + "1";
			pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
				"('" + TABLE1_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String pathSort2 = newPath.toString() + "2";
			pigJob = pigServer.store("table2", pathSort2, TableStorer.class.getCanonicalName() +
				"('" + TABLE2_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String pathSort3 = newPath.toString() + "3";
			pigJob = pigServer.store("sort3", pathSort3, TableStorer.class.getCanonicalName() +
				"('" + TABLE3_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			String queryLoad = "records1 = LOAD '"
		        + pathSort1 + ","
		        + pathSort2 + ","
		        + pathSort3
		        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('e,source_table,a,b,c,d,f,m1,str1', 'sorted');";
			pigServer.registerQuery(queryLoad);
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("The table is not sorted"));
		}
	}
	
	@Test
	public void union_error_sort_key() throws ExecException {
		//
		// Test sorted union error handling with mixed sort keys (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a");  // input table and sort keys
		inputTables.put("table2", "b");  // input table and sort keys
		inputTables.put("table3", "e");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "source_table,a,b,e");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("does not exist in schema"));
		}
	}
	
	@Test
	public void union_error_sort_key_partial() throws ExecException {
		//
		// Test sorted union error handling with mixed multiple sort keys (Negative test)
		//
		IOException exception = null;
		
		// Create input tables for order preserve union
		Map<String, String> inputTables = new HashMap<String, String>();  // Input two or more tables
		inputTables.put("table1", "a,e");  // input table and sort keys
		inputTables.put("table2", "a,b");  // input table and sort keys
		
		try {
			// Test with input tables and provided output columns
			testOrderPreserveUnion(inputTables, "a,e,b");
			
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("The table is not properly sorted"));
		}
	}
	
	@Test
	public void test_pig_foreach_error() throws ExecException, IOException {
		//
		// Test sorted union after Pig foreach that does not change the sort order (Negative test)
		// The union will fail as no sort info is added when store is done after Pig foreach statement
		//
		IOException exception = null;
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Table after pig foreach
		String foreach2 = "foreachsort2 = FOREACH sort2 GENERATE a as a, b as b, c as c, d as d, e as e, f as f, m1 as m1;";
		pigServer.registerQuery(foreach2);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + "1";
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + TABLE1_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + "2";
		pigJob = pigServer.store("foreachsort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + TABLE2_STORAGE + "')");
		Assert.assertNull(pigJob.getException());
		
		try {
			String queryLoad = "records1 = LOAD '"
				+ pathSort1 + ","
				+ pathSort2
				+	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,source_table,b,c,d,e,f,m1', 'sorted');";
			
			pigServer.registerQuery(queryLoad);
		} catch (IOException e) {
			exception = e;
		} finally {
			//System.out.println(getStackTrace(exception));
			Assert.assertNotNull(exception);
			Assert.assertTrue(getStackTrace(exception).contains("The table is not sorted"));
		}
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
