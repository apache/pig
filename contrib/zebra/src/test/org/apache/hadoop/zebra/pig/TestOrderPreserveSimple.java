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


public class TestOrderPreserveSimple extends BaseTestCase {
	
	final static String STR_SCHEMA1 = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String STR_STORAGE1 = "[a, b, c]; [d, e, f]; [m1#{a}]";
	
	static int fileId = 0;
	
	protected static ExecJob pigJob;
	
	private static Path pathTable1;
	private static Path pathTable2;
	
	private static Object[][] table1;
	private static Object[][] table2;
	
	private static Map<String, String> m1;
	private static Map<String, String> m2;

  @BeforeClass
  public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestOrderPreserveSimple1");
    pathTable2 = getTableFullPath("TestOrderPreserveSimple2");    
    removeDir(pathTable1);
    removeDir(pathTable2);
    
		// Create table1 data
		m1 = new HashMap<String, String>();
		m1.put("a","m1-a");
		m1.put("b","m1-b");
		
		table1 = new Object[][]{
			{5,		-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),		m1},
			{-1,	3.25f,	1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),		m1},
			{1001,	100.0f,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),		m1},
			{1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),		m1},
			{1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),		m1},
			{1001,	50.0f,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),		m1},
			{1001,	52.0f,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),		m1},
			{1002,	28.0f,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),	m1},
			{1000,	0.0f,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),	m1} };
		
		// Create table1
		createTable(pathTable1, STR_SCHEMA1, STR_STORAGE1, table1);
		
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
		createTable(pathTable2, STR_SCHEMA1, STR_STORAGE1, table2);
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
 
	private Iterator<Tuple> orderPreserveUnion(String sortkey, String columns) throws IOException {
		//
		// Test sorted union with two tables that are different
		//
		
		// Load tables
		String query1 = "table1 = LOAD '" + pathTable1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query1);
		
		String query2 = "table2 = LOAD '" + pathTable2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query2);
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + sortkey + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + sortkey + " ;";
		pigServer.registerQuery(orderby2);
		
		Path newPath = new Path(getCurrentMethodName());
		
		// Store sorted tables
		String pathSort1 = newPath.toString() + ++fileId;  // increment fileId suffix
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('" + STR_STORAGE1 + "')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = newPath.toString() + ++fileId;  // increment fileId suffix
		pigJob = pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('" + STR_STORAGE1 + "')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('" + columns + "', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Return iterator
		Iterator<Tuple> it1 = pigServer.openIterator("records1");
		return it1;
	}
	
	@Test
	public void test_union_int() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: int
		//
		orderPreserveUnion("a", "a,b,c,d,e,f,m1, source_table");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable
	  		= new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, -1,   3.25f,  1000L,  50e+2,  "zebra",  new DataByteArray("zebra"), m1);
    addResultRow(rows,  5,    -3.25f, 1001L,  51e+2,  "Zebra",  new DataByteArray("Zebra"), m1);  
    addResultRow(rows, 1000, 0.0f, 1002L,  52e+2,  "hadoop", new DataByteArray("hadoop"),m1);
    addResultRow(rows, 1001, 100.0f, 1003L,  50e+2,  "Apple",  new DataByteArray("Apple"), m1);
    addResultRow(rows, 1001, 101.0f, 1001L,  50e+2,  "apple",  new DataByteArray("apple"), m1);
    addResultRow(rows, 1001, 101.0f, 1001L,  50e+2,  "apple",  new DataByteArray("apple"), m1);
    addResultRow(rows, 1001, 50.0f,  1000L,  50e+2,  "Pig",    new DataByteArray("Pig"), m1);
    addResultRow(rows, 1001, 52.0f,  1001L,  50e+2,  "pig",    new DataByteArray("pig"), m1);
    addResultRow(rows, 1002, 28.0f,  1000L,  50e+2,  "Hadoop", new DataByteArray("Hadoop"),m1);
		resultTable.put(0, rows);
    
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, -1,   -99.0f, 1002L,  51e+2,  "orange", new DataByteArray("orange"),m2);
    addResultRow(rows, 15,   56.0f,  1004L,  50e+2,  "green",  new DataByteArray("green"), m2);
    addResultRow(rows, 1001, 100.0f, 1003L,  55e+2,  "white",  new DataByteArray("white"), m2);
    addResultRow(rows, 1001, 102.0f, 1001L,  52e+2,  "purple", new DataByteArray("purple"),m2);
    addResultRow(rows, 1001, 50.0f,  1008L,  52e+2,  "gray",   new DataByteArray("gray"),  m2);
    addResultRow(rows, 1001, 53.0f,  1001L,  52e+2,  "brown",  new DataByteArray("brown"), m2);
    addResultRow(rows, 2000, 33.0f,  1006L,  52e+2,  "beige",  new DataByteArray("beige"), m2);
    resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 7, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_int_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: int
		//
		orderPreserveUnion("a", "source_table,a,b,c,d,e,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable
		  = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,  -1,   3.25f,  1000L,  50e+2,  "zebra",  new DataByteArray("zebra"), m1);
		addResultRow(rows, 0,	5,		-3.25f,	1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),	m1);	
		addResultRow(rows, 0,	1000,	0.0f,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),m1);
		addResultRow(rows, 0,	1001,	100.0f,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),	m1);
		addResultRow(rows, 0,	1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	1001,	101.0f,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	1001,	50.0f,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),	m1);
		addResultRow(rows, 0,	1001,	52.0f,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,	1002,	28.0f,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,	-1,		-99.0f,	1002L,	51e+2,	"orange",	new DataByteArray("orange"),m2);
		addResultRow(rows, 1,  15,   56.0f,  1004L,  50e+2,  "green",  new DataByteArray("green"), m2);
		addResultRow(rows, 1,  1001, 100.0f, 1003L,  55e+2,  "white",  new DataByteArray("white"), m2);
		addResultRow(rows, 1,  1001, 102.0f, 1001L,  52e+2,  "purple", new DataByteArray("purple"),m2);
		addResultRow(rows, 1,  1001, 50.0f,  1008L,  52e+2,  "gray",   new DataByteArray("gray"),  m2);
		addResultRow(rows, 1,  1001, 53.0f,  1001L,  52e+2,  "brown",  new DataByteArray("brown"), m2);
		addResultRow(rows, 1,  2000, 33.0f,  1006L,  52e+2,  "beige",  new DataByteArray("beige"), m2);
		resultTable.put(1, rows);
		
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_float_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: float
		//
		orderPreserveUnion("b", "source_table,b,a,c,d,e,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable =
      new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,	-3.25f,	5,		1001L,	51e+2,	"Zebra",	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, 0,	0.0f,	1000,	1002L,	52e+2,	"hadoop",	new DataByteArray("hadoop"),m1);
		addResultRow(rows, 0,	3.25f,	-1,		1000L,	50e+2,	"zebra",	new DataByteArray("zebra"),	m1);	
		addResultRow(rows, 0,	28.0f,	1002,	1000L,	50e+2,	"Hadoop",	new DataByteArray("Hadoop"),m1);
		addResultRow(rows, 0,	50.0f,	1001,	1000L,	50e+2,	"Pig",		new DataByteArray("Pig"),	m1);	
		addResultRow(rows, 0,	52.0f,	1001,	1001L,	50e+2,	"pig",		new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,	100.0f,	1001,	1003L,	50e+2,	"Apple",	new DataByteArray("Apple"),	m1);
		addResultRow(rows, 0,	101.0f,	1001,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	101.0f,	1001,	1001L,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,	-99.0f,	-1,		1002L,	51e+2,	"orange",	new DataByteArray("orange"),m2);
		addResultRow(rows, 1,  33.0f,  2000, 1006L,  52e+2,  "beige",  new DataByteArray("beige"), m2);
		addResultRow(rows, 1,  50.0f,  1001, 1008L,  52e+2,  "gray",   new DataByteArray("gray"),  m2);
		addResultRow(rows, 1,  53.0f,  1001, 1001L,  52e+2,  "brown",  new DataByteArray("brown"), m2);
    addResultRow(rows, 1,  56.0f,  15,   1004L,  50e+2,  "green",  new DataByteArray("green"), m2);
    addResultRow(rows, 1,  100.0f, 1001, 1003L,  55e+2,  "white",  new DataByteArray("white"), m2);
    addResultRow(rows, 1,  102.0f, 1001, 1001L,  52e+2,  "purple", new DataByteArray("purple"),m2);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_long_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: long
		//
		orderPreserveUnion("c", "source_table,c,a,b,d,e,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, 0,  1000L,  -1,   3.25f,  50e+2,  "zebra",  new DataByteArray("zebra"), m1);
    addResultRow(rows, 0,  1000L,  1001, 50.0f,  50e+2,  "Pig",    new DataByteArray("Pig"), m1);
    addResultRow(rows, 0,  1000L,  1002, 28.0f,  50e+2,  "Hadoop", new DataByteArray("Hadoop"),m1);
		addResultRow(rows, 0,	1001L,	5,		-3.25f,	51e+2,	"Zebra",	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, 0,	1001L,	1001,	101.0f,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	1001L,	1001,	101.0f,	50e+2,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,	1001L,	1001,	52.0f,	50e+2,	"pig",		new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,	1002L,	1000,	0.0f,	52e+2,	"hadoop",	new DataByteArray("hadoop"),m1);
		addResultRow(rows, 0,	1003L,	1001,	100.0f,	50e+2,	"Apple",	new DataByteArray("Apple"),	m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,	1001L,	1001,	102.0f,	52e+2,	"purple",	new DataByteArray("purple"),m2);
		addResultRow(rows, 1,  1001L,  1001, 53.0f,  52e+2,  "brown",  new DataByteArray("brown"), m2);
		addResultRow(rows, 1,  1002L,  -1,   -99.0f, 51e+2,  "orange", new DataByteArray("orange"),m2);
		addResultRow(rows, 1,  1003L,  1001, 100.0f, 55e+2,  "white",  new DataByteArray("white"), m2);
		addResultRow(rows, 1,  1004L,  15,   56.0f,  50e+2,  "green",  new DataByteArray("green"), m2);
    addResultRow(rows, 1,  1006L,  2000, 33.0f,  52e+2,  "beige",  new DataByteArray("beige"), m2);
    addResultRow(rows, 1,  1008L,  1001, 50.0f,  52e+2,  "gray",   new DataByteArray("gray"),  m2);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_double_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: double
		//
		orderPreserveUnion("d", "source_table,d,a,b,c,e,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    addResultRow(rows, 0,50e+2,  -1,   3.25f,  1000L,  "zebra",  new DataByteArray("zebra"), m1);
		addResultRow(rows, 0,50e+2,	1001,	100.0f,	1003L,	"Apple",	new DataByteArray("Apple"),	m1);
		addResultRow(rows, 0,50e+2,	1001,	101.0f,	1001L,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,50e+2,	1001,	101.0f,	1001L,	"apple",	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,50e+2,	1001,	50.0f,	1000L,	"Pig",		new DataByteArray("Pig"),	m1);
		addResultRow(rows, 0,50e+2,	1001,	52.0f,	1001L,	"pig",		new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,50e+2,	1002,	28.0f,	1000L,	"Hadoop",	new DataByteArray("Hadoop"),m1);
		addResultRow(rows, 0,51e+2,	5,		-3.25f,	1001L,	"Zebra",	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, 0,52e+2,	1000,	0.0f,	1002L,	"hadoop",	new DataByteArray("hadoop"),m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,50e+2,	15,		56.0f,	1004L,	"green",	new DataByteArray("green"),	m2);
		addResultRow(rows, 1,51e+2,  -1,   -99.0f, 1002L,  "orange", new DataByteArray("orange"),m2);
		addResultRow(rows, 1,52e+2,  1001, 102.0f, 1001L,  "purple", new DataByteArray("purple"),m2);
		addResultRow(rows, 1,52e+2,  1001, 50.0f,  1008L,  "gray",   new DataByteArray("gray"),  m2);
    addResultRow(rows, 1,52e+2,  1001, 53.0f,  1001L,  "brown",  new DataByteArray("brown"), m2);
    addResultRow(rows, 1,52e+2,  2000, 33.0f,  1006L,  "beige",  new DataByteArray("beige"), m2);
    addResultRow(rows, 1,55e+2,  1001, 100.0f, 1003L,  "white",  new DataByteArray("white"), m2);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_string_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: string
		//
		orderPreserveUnion("e", "source_table,e,a,b,c,d,f,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,"Apple",	1001,	100.0f,	1003L,	50e+2,	new DataByteArray("Apple"),	m1);
		addResultRow(rows, 0,"Hadoop",	1002,	28.0f,	1000L,	50e+2,	new DataByteArray("Hadoop"),m1);
		addResultRow(rows, 0,"Pig",		1001,	50.0f,	1000L,	50e+2,	new DataByteArray("Pig"),	m1);
		addResultRow(rows, 0,"Zebra",	5,		-3.25f,	1001L,	51e+2,	new DataByteArray("Zebra"),	m1);
		addResultRow(rows, 0,"apple",	1001,	101.0f,	1001L,	50e+2,	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,"apple",	1001,	101.0f,	1001L,	50e+2,	new DataByteArray("apple"),	m1);
		addResultRow(rows, 0,"hadoop",	1000,	0.0f,	1002L,	52e+2,	new DataByteArray("hadoop"),m1);
		addResultRow(rows, 0,"pig",		1001,	52.0f,	1001L,	50e+2,	new DataByteArray("pig"),	m1);
		addResultRow(rows, 0,"zebra",	-1,		3.25f,	1000L,	50e+2,	new DataByteArray("zebra"),	m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,"beige",  2000, 33.0f,  1006L,  52e+2,  new DataByteArray("beige"), m2);
    addResultRow(rows, 1,"brown",  1001, 53.0f,  1001L,  52e+2,  new DataByteArray("brown"), m2);
    addResultRow(rows, 1,"gray",   1001, 50.0f,  1008L,  52e+2,  new DataByteArray("gray"),  m2);
    addResultRow(rows, 1,"green",  15,   56.0f,  1004L,  50e+2,  new DataByteArray("green"), m2);
    addResultRow(rows, 1,"orange", -1,   -99.0f, 1002L,  51e+2,  new DataByteArray("orange"),m2);
    addResultRow(rows, 1,"purple", 1001, 102.0f, 1001L,  52e+2,  new DataByteArray("purple"),m2);
    addResultRow(rows, 1,"white",  1001, 100.0f, 1003L,  55e+2,  new DataByteArray("white"), m2);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
	}
	
	@Test
	public void test_union_bytes_source() throws ExecException, IOException {
		//
		// Test sorted union with two tables with: bytes
		//
		orderPreserveUnion("f", "source_table,f,a,b,c,d,e,m1");
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 0,new DataByteArray("Apple"),	1001,	100.0f,	1003L,	50e+2,	"Apple",	m1);
		addResultRow(rows, 0,new DataByteArray("Hadoop"),1002,	28.0f,	1000L,	50e+2,	"Hadoop",	m1);
		addResultRow(rows, 0,new DataByteArray("Pig"),	1001,	50.0f,	1000L,	50e+2,	"Pig",		m1);
		addResultRow(rows, 0,new DataByteArray("Zebra"),	5,		-3.25f,	1001L,	51e+2,	"Zebra",	m1);	
		addResultRow(rows, 0,new DataByteArray("apple"),	1001,	101.0f,	1001L,	50e+2,	"apple",	m1);
		addResultRow(rows, 0,new DataByteArray("apple"),	1001,	101.0f,	1001L,	50e+2,	"apple",	m1);
		addResultRow(rows, 0,new DataByteArray("hadoop"),1000,	0.0f,	1002L,	52e+2,	"hadoop",	m1);
		addResultRow(rows, 0,new DataByteArray("pig"),	1001,	52.0f,	1001L,	50e+2,	"pig",		m1);
		addResultRow(rows, 0,new DataByteArray("zebra"),	-1,		3.25f,	1000L,	50e+2,	"zebra",	m1);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, 1,new DataByteArray("beige"), 2000, 33.0f,  1006L,  52e+2,  "beige",  m2);
    addResultRow(rows, 1,new DataByteArray("brown"), 1001, 53.0f,  1001L,  52e+2,  "brown",  m2);
    
    addResultRow(rows, 1,new DataByteArray("gray"),  1001, 50.0f,  1008L,  52e+2,  "gray",   m2);
    addResultRow(rows, 1,new DataByteArray("green"), 15,   56.0f,  1004L,  50e+2,  "green",  m2);
    addResultRow(rows, 1,new DataByteArray("orange"),-1,   -99.0f, 1002L,  51e+2,  "orange", m2);
    addResultRow(rows, 1,new DataByteArray("purple"),1001, 102.0f, 1001L,  52e+2,  "purple", m2);
    addResultRow(rows, 1,new DataByteArray("white"), 1001, 100.0f, 1003L,  55e+2,  "white",  m2);
		resultTable.put(1, rows);
    
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 1, 0, it);
		
		Assert.assertEquals(numbRows, table1.length + table2.length);
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
	public String getCurrentMethodName() {
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
