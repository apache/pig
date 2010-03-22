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
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;

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
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestMergeJoinPartial extends BaseTestCase {
	
	final static String STR_SCHEMA1 = "a:int,b:float,c:long,d:double,e:string,f:bytes,m1:map(string)";
	final static String STR_STORAGE1 = "[a, b, c]; [e, f]; [m1#{a}]";
	
	static int fileId = 0;
	
	private static Path pathTable1;
	private static Path pathTable2;
	
	private static Object[][] table1;
	private static Object[][] table2;
	
	@BeforeClass
	public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestMergeJoinPartial1");
    pathTable2 = getTableFullPath("TestMergeJoinPartial2");    
    removeDir(pathTable1);
    removeDir(pathTable2);
		
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
		createTable(pathTable1, STR_SCHEMA1, STR_STORAGE1, table1);
		
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
		createTable(pathTable2, STR_SCHEMA1, STR_STORAGE1, table2);
		
		// Load table1
		String query1 = "table1 = LOAD '" + pathTable1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query1);
		
		// Load table2
		String query2 = "table2 = LOAD '" + pathTable2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
		pigServer.registerQuery(query2);
		
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
	public void test_merge_joint_17() throws ExecException, IOException {
		//
		// Multiple join where join keys are partial, and the order of keys is honored
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a,b,c" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a,b,c" + " ;";
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
		String join = "joinRecords = JOIN records1 BY " + "(" + "a,b,c" + ")" + " , records2 BY " + "("+ "a,b,c" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		// Verify merged tables
		ArrayList<ArrayList<Object>> resultTable = new ArrayList<ArrayList<Object>>();
		
		addResultRow(resultTable, table1[2], table2[2]);  // set expected values for row1
		
		Iterator<Tuple> it = pigServer.openIterator("joinRecords");
		verifyTable(resultTable, it);
	}
	
	@Test
	public void test_merge_joint_22() throws ExecException, IOException {
		//
		// Multiple join where join keys are partial, and the order of keys is honored
		//
		// Known bug with partial key join
		// - Need to add verification to this test once bug is fixed
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a,b,c" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a,b,c" + " ;";
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
		String join = "joinRecords = JOIN records1 BY " + "(" + "a,b" + ")" + " , records2 BY " + "("+ "a,b" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		printTable("joinRecords");
	}
	
	@Test
	public void test_merge_joint_23() throws ExecException, IOException {
		//
		// Multiple join where join keys are partial, and the order of keys is honored
		//
		// Known bug with partial key join
		// - Need to add verification to this test once bug is fixed
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a,b,c" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a,b,c" + " ;";
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
		
		printTable("joinRecords");
	}
	
	@Test(expected = IOException.class)
	public void test_merge_joint_24() throws ExecException, IOException {
		//
		// Multiple join where join keys are partial, and the order of keys is honored
		// (negative test)
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a,b,c" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a,b,c" + " ;";
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
		String join = "joinRecords = JOIN records1 BY " + "(" + "a,c" + ")" + " , records2 BY " + "("+ "a,c" + ")" +
			" USING \"merge\";";
		pigServer.registerQuery(join);
		
		pigServer.openIterator("joinRecords");  // get iterator to trigger error
	}
	
	public void printTable(String tablename) throws IOException {
		//
		// Print Pig Table (for debugging)
		//
		Iterator<Tuple> it1 = pigServer.openIterator(tablename);
		Tuple RowValue1 = null;
		while (it1.hasNext()) {
			RowValue1 = it1.next();
			System.out.println();
			
			for (int i = 0; i < RowValue1.size(); ++i) {
				System.out.println("DEBUG: " + tablename + " RowValue.get(" + i + ") = " + RowValue1.get(i));
				
			}
		}
	}
	
	public void addResultRow(ArrayList<ArrayList<Object>> resultTable, Object[] leftRow, Object[] rightRow) {
		//
		// Add a row to expected results table
		//
		ArrayList<Object> resultRow = new ArrayList<Object>();
		
		for (int i=0; i<leftRow.length; ++i)
			resultRow.add(leftRow[i]);
		for (int i=0; i<rightRow.length; ++i)
			resultRow.add(rightRow[i]);
		
		resultTable.add(resultRow);
	}
	
	public void verifyTable(ArrayList<ArrayList<Object>> resultTable, Iterator<Tuple> it) throws IOException {
		//
		// Verify expected results table to returned test case table
		//
		Tuple RowValues;
		int rowIndex = 0;
		
		while (it.hasNext()) {
			RowValues = it.next();
			ArrayList<Object> resultRow = resultTable.get(rowIndex);
			Assert.assertEquals(resultRow.size(), RowValues.size());  // verify expected tuple count
			System.out.println();
			
			for (int i = 0; i < RowValues.size(); ++i) {
				System.out.println("DEBUG: resultTable " + " RowValue.get(" + i + ") = " + RowValues.get(i) +
						" " + resultRow.get(i));
				Assert.assertEquals(resultRow.get(i), RowValues.get(i));  // verify each row value
			}
			++rowIndex;
		}
		Assert.assertEquals(resultTable.size(), rowIndex);  // verify expected row count
	}
	
}
