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
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestOrderPreserveMultiTable {
	
	final static int NUMB_TABLE = 10;  		// number of tables for stress test
	final static int NUMB_TABLE_ROWS = 5;	// number of rows for each table
	
	final static String TABLE_SCHEMA = "int1:int,str1:string,byte1:bytes";
	final static String TABLE_STORAGE = "[int1,str1,byte1]";
	
	static int fileId = 0;
	static int sortId = 0;
	
	protected static ExecType execType = ExecType.MAPREDUCE;
	private static MiniCluster cluster;
	protected static PigServer pigServer;
	protected static ExecJob pigJob;
	
	private static ArrayList<Path> pathTables;
	private static int totalTableRows =0;
	
	private static Configuration conf;
	private static FileSystem fs;
	
	private static String zebraJar;
	private static String whichCluster;
	
	@BeforeClass
	public static void setUp() throws Exception {
		if (System.getProperty("hadoop.log.dir") == null) {
			String base = new File(".").getPath(); // getAbsolutePath();
			System.setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
		}
		
		// if whichCluster is not defined, or defined something other than
		// "realCluster" or "miniCluster", set it to "realCluster"
		if (System.getProperty("whichCluster") == null
				|| ((!System.getProperty("whichCluster")
						.equalsIgnoreCase("realCluster")) && (!System.getProperty(
						"whichCluster").equalsIgnoreCase("miniCluster")))) {
			System.setProperty("whichCluster", "miniCluster");
			whichCluster = System.getProperty("whichCluster");
		} else {
			whichCluster = System.getProperty("whichCluster");
		}
		
		System.out.println("cluster: " + whichCluster);
		if (whichCluster.equalsIgnoreCase("realCluster")
				&& System.getenv("HADOOP_HOME") == null) {
			System.out.println("Please set HADOOP_HOME");
			System.exit(0);
		}
		
		conf = new Configuration();
		
		if (whichCluster.equalsIgnoreCase("realCluster")
				&& System.getenv("USER") == null) {
			System.out.println("Please set USER");
			System.exit(0);
		}
		zebraJar = System.getenv("HADOOP_HOME") + "/../jars/zebra.jar";
		File file = new File(zebraJar);
		if (!file.exists() && whichCluster.equalsIgnoreCase("realCulster")) {
			System.out.println("Please put zebra.jar at hadoop_home/../jars");
			System.exit(0);
		}
		
		if (whichCluster.equalsIgnoreCase("realCluster")) {
			pigServer = new PigServer(ExecType.MAPREDUCE, ConfigurationUtil
					.toProperties(conf));
			pigServer.registerJar(zebraJar);
			
			pathTables = new ArrayList<Path>();
			for (int i=0; i<NUMB_TABLE; ++i) {
				Path pathTable = new Path("/user/" + System.getenv("USER")
						+ "/TestOderPerserveMultiTable" + i);
				pathTables.add(pathTable);
				removeDir(pathTable);
			}
			fs = pathTables.get(0).getFileSystem(conf);
		}
		
		if (whichCluster.equalsIgnoreCase("miniCluster")) {
			if (execType == ExecType.MAPREDUCE) {
				cluster = MiniCluster.buildCluster();
				pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
				fs = cluster.getFileSystem();
				
				pathTables = new ArrayList<Path>();
				for (int i=0; i<NUMB_TABLE; ++i) {
					Path pathTable = new Path(fs.getWorkingDirectory()
							+ "/TestOderPerserveMultiTable" + i);
					pathTables.add(pathTable);
					removeDir(pathTable);
				}
			} else {
				pigServer = new PigServer(ExecType.LOCAL);
			}
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
	
	public static void removeDir(Path outPath) throws IOException {
		String command = null;
		if (whichCluster.equalsIgnoreCase("realCluster")) {
			command = System.getenv("HADOOP_HOME") +"/bin/hadoop fs -rmr " + outPath.toString();
		}
		else{
			command = "rm -rf " + outPath.toString();
		}
		Runtime runtime = Runtime.getRuntime();
		Process proc = runtime.exec(command);
		int exitVal = -1;
		try {
			exitVal = proc.waitFor();
		} catch (InterruptedException e) {
			System.err.println(e);
		}
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
		testOrderPreserveUnion(inputTables, "int1", "int1, source_table, str1, byte1");
		
		// Create results table for verification
		ArrayList<ArrayList<Object>> resultTable = new ArrayList<ArrayList<Object>>();
		for (int i=0; i<NUMB_TABLE; ++i) {
			for (int j=0; j<NUMB_TABLE_ROWS; ++j) {
				ArrayList<Object> resultRow = new ArrayList<Object>();
				
				resultRow.add(i);	// int1
				resultRow.add(i);	// source_table
				resultRow.add(new String("string" + j));	// str1
				resultRow.add(new DataByteArray("byte" + (NUMB_TABLE_ROWS - j)));	// byte1
				
				resultTable.add(resultRow);
			}
		}
		
		// Verify union table
		Iterator<Tuple> it = pigServer.openIterator("records1");
		int numbRows = verifyTable(resultTable, 0, it);
		
		Assert.assertEquals(totalTableRows, numbRows);
		
		// Print Table
		//printTable("records1");
	}
	
	/**
	 * Verify union output table with expected results
	 * 
	 */
	private int verifyTable(ArrayList<ArrayList<Object>> resultTable, int keyColumn, Iterator<Tuple> it) throws IOException {
		int numbRows = 0;
		int index = 0;
		Object value = resultTable.get(index).get(keyColumn);  // get value of primary key
		
		while (it.hasNext()) {
			Tuple rowValues = it.next();
			
			// If last primary sort key does match then search for next matching key
			if (! compareObj(value, rowValues.get(keyColumn))) {
				int subIndex = index + 1;
				while (subIndex < resultTable.size()) {
					if ( ! compareObj(value, resultTable.get(subIndex).get(keyColumn)) ) {  // found new key
						index = subIndex;
						value = resultTable.get(index).get(keyColumn);
						break;
					}
					++subIndex;
				}
				Assert.assertEquals("Table comparison error for row : " + numbRows + " - no key found for : "
					+ rowValues.get(keyColumn), value, rowValues.get(keyColumn));
			}
			// Search for matching row with this primary key
			int subIndex = index;
			
			while (subIndex < resultTable.size()) {
				// Compare row
				ArrayList<Object> resultRow = resultTable.get(subIndex);
				if ( compareRow(rowValues, resultRow) )
					break; // found matching row
				++subIndex;
				Assert.assertEquals("Table comparison error for row : " + numbRows + " - no matching row found for : "
					+ rowValues.get(keyColumn), value, resultTable.get(subIndex).get(keyColumn));
			}
			++numbRows;
		}
		Assert.assertEquals(resultTable.size(), numbRows);  // verify expected row count
		return numbRows;
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
