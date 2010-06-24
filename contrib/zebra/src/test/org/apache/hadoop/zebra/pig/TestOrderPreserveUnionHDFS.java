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
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.zebra.BaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestOrderPreserveUnionHDFS extends BaseTestCase {
	
	final static String STR_SCHEMA1 = "a:string,b:string,c:string";
	final static String STR_STORAGE1 = "[a, b]; [c]";
	
	static int fileId = 0;
	
	protected static ExecJob pigJob;
	
	private static Path pathTable1;
	private static Path pathTable2;
	
	private static Object[][] table1;
	private static Object[][] table2;	
	
	@BeforeClass
	public static void setUp() throws Exception {
	  init();
	  
    pathTable1 = getTableFullPath("TestOrderPerserveSimple1");
    pathTable2 = getTableFullPath("TestOrderPerserveSimple2");
	  removeDir(pathTable1);
		removeDir(pathTable2);
				
		// Create table1 data
		table1 = new Object[][] {
			{"a1",	"z",	"5"},
			{"a2",	"r",	"4"},
			{"a3",	"e",	"3"},
			{"a4",	"a",	"1"} };
		
		// Create table1
		createTable(pathTable1, STR_SCHEMA1, STR_STORAGE1, table1);
		
		// Create table2 data
		table2 = new Object[][] {
			{"b1",	"a",	"a"},
			{"b2",	"a",	"a"},
			{"b3",	"a",	"a"},
			{"b4",	"a",	"a"} };
		
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
	public void test_sorted_table_union_hdfs() throws ExecException, IOException {
		//
		// Test sorted union with two tables that are different and that use
		// hdfs file URLs in Pig LOAD statement
		//
		
		// Sort tables
		String orderby1 = "sort1 = ORDER table1 BY " + "a" + " ;";
		pigServer.registerQuery(orderby1);
		
		String orderby2 = "sort2 = ORDER table2 BY " + "a" + " ;";
		pigServer.registerQuery(orderby2);
		
		// Store sorted tables
		++fileId;  // increment filename suffix
		String pathSort1 = "TestOrderPerserveSimple1" + Integer.toString(fileId);
	  removeDir(getTableFullPath(pathSort1));
		pigJob = pigServer.store("sort1", pathSort1, TableStorer.class.getCanonicalName() +
			"('[a, b]; [c]')");
		Assert.assertNull(pigJob.getException());
		
		String pathSort2 = "TestOrderPerserveSimple2" + Integer.toString(fileId);
	  removeDir(getTableFullPath(pathSort2));
		pigJob = pigServer.store("sort2", pathSort2, TableStorer.class.getCanonicalName() +
			"('[a, b]; [c]')");
		Assert.assertNull(pigJob.getException());
		
		String queryLoad = "records1 = LOAD '"
	        + pathSort1 + ","
	        + pathSort2
	        +	"' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c, source_table', 'sorted');";
		
		System.out.println("queryLoad: " + queryLoad);
		
		pigServer.registerQuery(queryLoad);
		
		// Verify union table
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable
    = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "a1",	"z",	"5", 0);
		addResultRow(rows, "a2",	"r",	"4", 0);
		addResultRow(rows, "a3",	"e",	"3", 0);
		addResultRow(rows, "a4",	"a",	"1", 0);
		resultTable.put(0, rows);
		
		rows = new ArrayList<ArrayList<Object>>();
		addResultRow(rows, "b1",	"a",	"a", 1);
		addResultRow(rows, "b2",	"a",	"a", 1);
		addResultRow(rows, "b3",	"a",	"a", 1);
		addResultRow(rows, "b4",	"a",	"a", 1);
		resultTable.put(1, rows);
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 3, it);
		
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
	
}
