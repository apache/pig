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
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

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

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestOrderPreserveMultiTableGlob extends BaseTestCase {
	
	final static int NUMB_TABLE = 10;  		// number of tables for stress test
	final static int NUMB_TABLE_ROWS = 5;	// number of rows for each table
	
	final static String TABLE_SCHEMA = "int1:int,str1:string,byte1:bytes";
	final static String TABLE_STORAGE = "[int1,str1,byte1]";
	
	static int fileId = 0;
	static int sortId = 0;
	
	protected static ExecJob pigJob;
	
	private static ArrayList<Path> pathTables;
	private static int totalTableRows =0;
	
	@BeforeClass
	public static void setUp() throws Exception {
		init();
		

		pathTables = new ArrayList<Path>();
		for (int i=0; i<NUMB_TABLE; ++i) {
			Path pathTable = getTableFullPath("TestOderPerserveMultiTable" + i);
			pathTables.add(pathTable);
			removeDir(pathTable);
		}
			
		
		// Create tables
		for (int i=0; i<NUMB_TABLE; ++i) {
			// Create table data
			Object[][] table = new Object[NUMB_TABLE_ROWS][3];  // three columns
			
			for (int j=0; j<NUMB_TABLE_ROWS; ++j) {
				table[j][0] = i;
				table[j][1] = new String("string" + j);
				table[j][2] = new DataByteArray("byte" + (NUMB_TABLE_ROWS - j));
				++totalTableRows;
			}
			// Create table
			createTable(pathTables.get(i), TABLE_SCHEMA, TABLE_STORAGE, table);
			
			// Load Table
			String query = "table" + i + " = LOAD '" + pathTables.get(i).toString() + 
					"' USING org.apache.hadoop.zebra.pig.TableLoader();";
			pigServer.registerQuery(query);
		}
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
	
	private Iterator<Tuple> testOrderPreserveUnion(ArrayList<String> inputTables, String sortkey, String columns)
				throws IOException {
		//
		// Test order preserve union from input tables and provided output columns
		//
		Assert.assertTrue("Table union requires two or more input tables", inputTables.size() >= 2);
		
		Path newPath = new Path(getCurrentMethodName());
		ArrayList<String> pathList = new ArrayList<String>();
		
		// Load and store each of the input tables
		for (int i=0; i<inputTables.size(); ++i) {
			String tablename = inputTables.get(i);
			String sortName = "sort" + ++sortId;
			
			// Sort tables
			String orderby = sortName + " = ORDER " + tablename + " BY " + sortkey + " ;";
			pigServer.registerQuery(orderby);
			
			String sortPath = new String(newPath.toString() + ++fileId);  // increment fileId suffix
			
			// Store sorted tables
			pigJob = pigServer.store(sortName, sortPath, TableStorer.class.getCanonicalName() +
				"('" + TABLE_STORAGE + "')");
			Assert.assertNull(pigJob.getException());
			
			pathList.add(sortPath);  // add table path to list
		}
		
		String paths = new String();
		
    paths += newPath.toString() + "{";
    fileId = 0;
		for (String path:pathList)
			paths += ++fileId + ",";
		paths = paths.substring(0, paths.lastIndexOf(","));  // remove trailing comma
    paths += "}";
    
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
	public void test_sorted_union_multi_table() throws ExecException, IOException {
		//
		// Test sorted union
		//
		
		// Create input tables for order preserve union
		ArrayList<String> inputTables = new ArrayList<String>();  // Input tables
		for (int i=0; i<NUMB_TABLE; ++i) {
			inputTables.add("table" + i);  // add input table
		}
		
		// Test with input tables and provided output columns
		testOrderPreserveUnion(inputTables, "int1", "int1, str1, byte1, source_table");
		
		// Create results table for verification
		HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable = 
		  new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		
		// The ordering from FileInputFormat glob expansion.
		int[] tblIndexList = {0, 9, 1, 2, 3, 4, 5, 6, 7, 8};
		
		for (int i=0; i<NUMB_TABLE; ++i) {
		  ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
			for (int j=0; j<NUMB_TABLE_ROWS; ++j) {
				ArrayList<Object> resultRow = new ArrayList<Object>();
				resultRow.add(tblIndexList[i]);	// int1
				resultRow.add(new String("string" + j));	// str1
				resultRow.add(new DataByteArray("byte" + (NUMB_TABLE_ROWS - j)));	// byte1
				rows.add(resultRow);
			}
			resultTable.put(i, rows);
		}
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, 3, it);
		
		Assert.assertEquals(totalTableRows, numbRows);
		
		// Print Table
		//printTable("records1");
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
