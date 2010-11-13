/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.zebra.mapred;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Assert;

/**
 * TestComparator
 * 
 * Utility for verifying tables created during Zebra Stress Testing
 * 
 */
public class ToolTestComparator extends BaseTestCase {

  final static String TABLE_SCHEMA = "count:int,seed:int,int1:int,int2:int,str1:string,str2:string,byte1:bytes,"
      + "byte2:bytes,float1:float,long1:long,double1:double,m1:map(string),r1:record(f1:string, f2:string),"
      + "c1:collection(record(a:string, b:string))";
  final static String TABLE_STORAGE = "[count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1];[m1#{a}];[r1,c1]";

  private static Random generator = new Random();

  protected static ExecJob pigJob;

  private static int totalNumbCols;
  private static long totalNumbVerifiedRows;

  /**
   * Setup and initialize environment
   */
  public static void setUp() throws Exception {
	  init();
  }

  /**
   * Verify load/store
   * 
   */
  public static void verifyLoad(String pathTable1, String pathTable2,
      int numbCols) throws IOException {
    System.out.println("verifyLoad()");

    // Load table1
    String query1 = "table1 = LOAD '" + pathTable1
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("verifyLoad() running query : " + query1);
    pigServer.registerQuery(query1);

    // Load table2
    String query2 = "table2 = LOAD '" + pathTable2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("verifyLoad() running query : " + query2);
    pigServer.registerQuery(query2);

    // Get metrics from first table
    Iterator<Tuple> it1 = pigServer.openIterator("table1");

    int numbCols1 = 0;
    long numbRows1 = 0;

    while (it1.hasNext()) {
      ++numbRows1; // increment row count
      Tuple rowValue = it1.next();
      numbCols1 = rowValue.size();
      if (numbCols != 0)
        Assert.assertEquals(
            "Verify failed - Table1 has wrong number of expected columns "
                + "\n row number : " + numbRows1 + "\n expected column size : "
                + numbCols + "\n actual columns size  : " + numbCols1,
            numbCols, numbCols1);
    }

    // Get metrics from second table
    Iterator<Tuple> it2 = pigServer.openIterator("table2");

    int numbCols2 = 0;
    long numbRows2 = 0;

    while (it2.hasNext()) {
      ++numbRows2; // increment row count
      Tuple rowValue = it2.next();
      numbCols2 = rowValue.size();
      if (numbCols != 0)
        Assert.assertEquals(
            "Verify failed - Table2 has wrong number of expected columns "
                + "\n row number : " + numbRows2 + "\n expected column size : "
                + numbCols + "\n actual columns size  : " + numbCols2,
            numbCols, numbCols2);
    }

    Assert
        .assertEquals(
            "Verify failed - Tables have different number row sizes "
                + "\n table1 rows : " + numbRows1 + "\n table2 rows : "
                + numbRows2, numbRows1, numbRows2);

    Assert.assertEquals(
        "Verify failed - Tables have different number column sizes "
            + "\n table1 column size : " + numbCols1
            + "\n table2 column size : " + numbCols2, numbCols1, numbCols2);

    System.out.println();
    System.out.println("Verify load - table1 columns : " + numbCols1);
    System.out.println("Verify load - table2 columns : " + numbCols2);
    System.out.println("Verify load - table1 rows : " + numbRows1);
    System.out.println("Verify load - table2 rows : " + numbRows2);
    System.out.println("Verify load - PASS");
  }

  /**
   * Verify table
   * 
   */
  public static void verifyTable(String pathTable1) throws IOException {
    System.out.println("verifyTable()");

    // Load table1
    String query1 = "table1 = LOAD '" + pathTable1
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("verifyTable() running query : " + query1);
    pigServer.registerQuery(query1);

    // Get metrics from table
    Iterator<Tuple> it1 = pigServer.openIterator("table1");

    int numbCols1 = 0;
    long numbRows1 = 0;

    System.out.println("DEBUG starting to iterate table1");

    while (it1.hasNext()) {
      ++numbRows1; // increment row count
      Tuple rowValue = it1.next();
      numbCols1 = rowValue.size();
    }

    System.out.println();
    System.out.println("Verify table columns : " + numbCols1);
    System.out.println("Verify table rows : " + numbRows1);
    System.out.println("Verify table complete");
  }

  /**
   * Verify sorted
   * 
   */
  public static void verifySorted(String pathTable1, String pathTable2,
      int sortCol, String sortKey, int numbCols, int rowMod)
      throws IOException, ParseException {
    System.out.println("verifySorted()");

    // Load table1
    String query1 = "table1 = LOAD '" + pathTable1
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("verifySorted() running query : " + query1);
    pigServer.registerQuery(query1);

    //
    // Get metrics from first table (unsorted)
    //
    Iterator<Tuple> it1 = pigServer.openIterator("table1");

    int numbCols1 = 0;
    long numbRows1 = 0;

    System.out.println("DEBUG starting to iterate table1");

    while (it1.hasNext()) {
      ++numbRows1; // increment row count
      Tuple rowValue = it1.next();
      numbCols1 = rowValue.size();
      if (numbCols != 0)
        Assert.assertEquals(
            "Verify failed - Table1 has wrong number of expected columns "
                + "\n row number : " + numbRows1 + "\n expected column size : "
                + numbCols + "\n actual columns size  : " + numbCols1,
            numbCols, numbCols1);
    }

    System.out.println();
    System.out.println("Verify unsorted table1 columns : " + numbCols1);
    System.out.println("Verify unsorted table1 rows : " + numbRows1);

    System.out.println("\nDEBUG starting to iterate table2");

    //
    // Get metrics from second table (sorted)
    //
    long numbRows2 = verifySortedTable(pathTable2, sortCol, sortKey, numbCols,
        rowMod, null);

    int numbCols2 = totalNumbCols;
    long numbVerifiedRows = totalNumbVerifiedRows;

    Assert
        .assertEquals(
            "Verify failed - Tables have different number row sizes "
                + "\n table1 rows : " + numbRows1 + "\n table2 rows : "
                + numbRows2, numbRows1, numbRows2);

    Assert.assertEquals(
        "Verify failed - Tables have different number column sizes "
            + "\n table1 column size : " + numbCols1
            + "\n table2 column size : " + numbCols2, numbCols1, numbCols2);

    System.out.println();
    System.out.println("Verify unsorted table1 columns : " + numbCols1);
    System.out.println("Verify sorted   table2 columns : " + numbCols2);
    System.out.println("Verify unsorted table1 rows : " + numbRows1);
    System.out.println("Verify sorted   table2 rows : " + numbRows2);
    System.out.println("Verify sorted - numb verified rows : "
        + numbVerifiedRows);
    System.out.println("Verify sorted - sortCol : " + sortCol);
    System.out.println("Verify sorted - PASS");
  }

  /**
   * Verify merge-join
   * 
   */
  public static void verifyMergeJoin(String pathTable1, int sortCol,
      String sortKey, int numbCols, int rowMod, String verifyDataColName) throws IOException,
      ParseException {
    System.out.println("verifyMergeJoin()");

    //
    // Verify sorted table
    //
    long numbRows = verifySortedTable(pathTable1, sortCol, sortKey, numbCols,
        rowMod, verifyDataColName);

    System.out.println();
    System.out.println("Verify merge-join   table columns : " + totalNumbCols);
    System.out.println("Verify merge-join   table rows : " + numbRows);
    System.out.println("Verify merge-join - numb verified rows : "
        + totalNumbVerifiedRows);
    System.out.println("Verify merge-join - sortCol : " + sortCol);
    System.out.println("Verify merge-join - PASS");
  }

  /**
   * Verify sorted-union
   * 
   */
  public static void verifySortedUnion(ArrayList<String> unionPaths,
      String pathTable1, int sortCol, String sortKey, int numbCols, int rowMod,
      String verifyDataColName) throws IOException, ParseException {
    System.out.println("verifySortedUnion()");

    long numbUnionRows = 0;
    ArrayList<Long> numbRows = new ArrayList<Long>();

    // Get number of rows from each of the input union tables
   for (int i = 0; i < unionPaths.size(); ++i) {
      // Load table1
      String query1 = "table1 = LOAD '" + unionPaths.get(i)
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
      System.out.println("verifySortedUnion() running query : " + query1);
      pigServer.registerQuery(query1);
      String orderby1 = "sort1 = ORDER table1 BY " + sortKey + " ;";
      System.out.println("orderby1 : " + orderby1);
      pigServer.registerQuery(orderby1);

      // Get metrics for each input sorted table
      Iterator<Tuple> it1 = pigServer.openIterator("sort1");
      long numbRows1 = 0;

      while (it1.hasNext()) {
        ++numbRows1; // increment row count
        Tuple rowValue = it1.next();
      }
      numbRows.add(numbRows1);
      numbUnionRows += numbRows1;
    }

    //
    // Verify sorted union table
    //
    long numbRows1 = verifySortedTable(pathTable1, sortCol, sortKey, numbCols,
        rowMod, verifyDataColName);

   
    //
    // Print all union input tables and rows for each
    //
    System.out.println();
    for (int i = 0; i < unionPaths.size(); ++i) {
      System.out.println("Input union table" + i + " path  : "
          + unionPaths.get(i));
      System.out.println("Input union table" + i + " rows  : "
          + numbRows.get(i));
    }
    System.out.println();
    System.out.println("Input union total rows   : " + numbUnionRows);

    System.out.println();
    System.out.println("Verify union - table columns : " + totalNumbCols);
    System.out.println("Verify union - table rows : " + numbRows1);
    System.out.println("Verify union - numb verified rows : "
        + totalNumbVerifiedRows);
    System.out.println("Verify union - sortCol : " + sortCol);

  /*  Assert.assertEquals(
        "Verify failed - sorted union table row comparison error "
            + "\n expected table rows : " + numbUnionRows
            + "\n actual table rows : " + numbRows1, numbUnionRows, numbRows1);
*/
    System.out.println("Verify union - PASS");
  }

  /**
   * Create unsorted table
   * 
   */
  public static void createtable(String pathTable1, long numbRows, int seed,
      boolean debug) throws ExecException, IOException, ParseException {
    System.out.println("createtable()");

    Path unsortedPath = new Path(pathTable1);

    // Remove old table (if present)
    removeDir(unsortedPath);

    // Create table
    BasicTable.Writer writer = new BasicTable.Writer(unsortedPath,
        TABLE_SCHEMA, TABLE_STORAGE, conf);

    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    TableInserter inserter = writer.getInserter("ins", false);

    Map<String, String> m1 = new HashMap<String, String>();

    Tuple tupRecord1; // record
    tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
        .getSchema()); // r1 schema

    DataBag bag1 = TypesUtils.createBag();
    Schema schColl = schema.getColumnSchema("c1").getSchema(); // c1 schema
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);

    int randRange = new Long(numbRows / 10).intValue(); // random range to allow
    // for duplicate values
    for (int i = 0; i < numbRows; ++i) {
      int random = generator.nextInt(randRange);

      TypesUtils.resetTuple(tuple); // reset row tuple
      m1.clear(); // reset map
      TypesUtils.resetTuple(tupRecord1); // reset record
      TypesUtils.resetTuple(tupColl1); // reset collection
      TypesUtils.resetTuple(tupColl2);
      bag1.clear();

      tuple.set(0, i); // count
      tuple.set(1, seed); // seed

      tuple.set(2, i); // int1
      tuple.set(3, random); // int2
      tuple.set(4, "string " + i); // str1
      tuple.set(5, "string random " + random); // str2
      tuple.set(6, new DataByteArray("byte " + i)); // byte1
      tuple.set(7, new DataByteArray("byte random " + random)); // byte2

      tuple.set(8, new Float(i * -1)); // float1 negative
      tuple.set(9, new Long(numbRows - i)); // long1 reverse
      tuple.set(10, new Double(i * 100)); // double1

      // insert map1
      m1.put("a", "m1");
      m1.put("b", "m1 " + i);
      tuple.set(11, m1);

      // insert record1
      tupRecord1.set(0, "r1 " + seed);
      tupRecord1.set(1, "r1 " + i);
      tuple.set(12, tupRecord1);

      // insert collection1
      // tupColl1.set(0, "c1 a " + seed);
      // tupColl1.set(1, "c1 a " + i);
      // bag1.add(tupColl1); // first collection item
      bag1.add(tupRecord1); // first collection item
      bag1.add(tupRecord1); // second collection item

      // tupColl2.set(0, "c1 b " + seed);
      // tupColl2.set(1, "c1 b " + i);
      // bag1.add(tupColl2); // second collection item

      tuple.set(13, bag1);

      inserter.insert(new BytesWritable(("key" + i).getBytes()), tuple);
    }
    inserter.close();
    writer.close();

    if (debug == true) {
      // Load tables
      String query1 = "table1 = LOAD '" + unsortedPath.toString()
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
      pigServer.registerQuery(query1);

      // Print Table
      printTable("table1");
    }

    System.out.println("Table Path : " + unsortedPath);
  }

  /**
   * Create sorted table
   * 
   */
  public static void createsortedtable(String pathTable1, String pathTable2,
      String sortString, boolean debug) throws ExecException, IOException {
    System.out.println("createsortedtable()");

    Path unsortedPath = new Path(pathTable1);
    Path sortedPath = new Path(pathTable2);

    // Remove old table (if present)
    removeDir(sortedPath);

    // Load tables
    String query1 = "table1 = LOAD '" + unsortedPath.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query1);

    // Sort table
    String orderby1 = "sort1 = ORDER table1 BY " + sortString + " ;";
    System.out.println("orderby1 : " + orderby1);
    pigServer.registerQuery(orderby1);

    // Store sorted tables
    pigJob = pigServer.store("sort1", sortedPath.toString(), TableStorer.class
        .getCanonicalName()
        + "('" + TABLE_STORAGE + "')");
    Assert.assertNull(pigJob.getException());

    // Print Table
    if (debug == true)
      printTable("sort1");

    System.out.println("Sorted Path : " + sortedPath);
  }

  /**
   * Delete table
   * 
   */
  public static void deleteTable(String pathTable1) throws ExecException,
      IOException {
    System.out.println("deleteTable()");

    Path tablePath = new Path(pathTable1);

    // Remove table (if present)
    removeDir(tablePath);

    System.out.println("Deleted Table Path : " + tablePath);
  }

  /**
   * Verify sorted table
   * 
   * Using BasicTable.Reader, read all table rows and verify that sortCol is in
   * sorted order
   * 
   */
  private static long verifySortedTable(String pathTable1, int sortCol,
      String sortKey, int numbCols, int rowMod, String verifyDataColName)
      throws IOException, ParseException {

    long numbRows = 0;

    Path tablePath = new Path(pathTable1);

    BasicTable.Reader reader = new BasicTable.Reader(tablePath, conf);
   
    JobConf conf1 = new JobConf(conf);
    System.out.println("sortKey: " + sortKey);
    TableInputFormat.setInputPaths(conf1, new Path(pathTable1));
 
    TableInputFormat.requireSortedTable(conf1, null);
    TableInputFormat tif = new TableInputFormat();
 
    SortedTableSplit split = (SortedTableSplit) tif.getSplits(conf1, 1)[0];
    
    TableScanner scanner = reader.getScanner(split.getBegin(), split.getEnd(), true);
    BytesWritable key = new BytesWritable();
    Tuple rowValue = TypesUtils.createTuple(scanner.getSchema());

    Object lastVal = null;
    int numbCols1 = 0;
    long numbVerifiedRows = 0;

    while (!scanner.atEnd()) {
      ++numbRows;
      scanner.getKey(key);

      scanner.getValue(rowValue);

      // Verify every nth row
      if ((numbRows % rowMod) == 0) {
        ++numbVerifiedRows;
        numbCols1 = rowValue.size();
        if (numbCols != 0)
          Assert.assertEquals(
              "Verify failed - Table1 has wrong number of expected columns "
                  + "\n row numberrr : " + numbRows
                  + "\n expected column size : " + numbCols
                  + "\n actual columns size  : " + numbCols1, numbCols,
              numbCols1);

        Object newVal = rowValue.get(sortCol);

        // Verify sort key is in sorted order
        Assert.assertTrue("Verify failed - Table1 sort comparison error "
            + "\n row number : " + numbRows + "\n sort column : " + sortCol
            + "\n sort column last value    : " + lastVal
            + "\n sort column current value : " + newVal, compareTo(newVal,
            lastVal) >= 0);

        lastVal = newVal; // save last compare value

        //
        // Optionally verify data
        //
       
        if (verifyDataColName != null && verifyDataColName.equals("long1")) {
          Object newValLong1 = rowValue.get(sortCol);
          if (numbRows < 2000){
            System.out.println("Row : "+ (numbRows-1) +" long1 value : "+newValLong1.toString());
          }
          Assert.assertEquals(
              "Verify failed - Union table data verification error for column name : "
                  + verifyDataColName + "\n row number : " + (numbRows-1)
                  + "\n expected value : " + (numbRows-1 + 4) / 4 + // long1 will start with value 1
                  "\n actual value   : " + newValLong1, (numbRows-1 + 4) / 4,
              newValLong1);

        }

        scanner.advance();
      }
      

    }
    
    System.out.println("\nTable Pathh : " + pathTable1);
    System.out.println("++++++++++Table Row number : " + numbRows);
    
   
    reader.close();
   
    totalNumbCols = numbCols1;
    totalNumbVerifiedRows = numbVerifiedRows;

    return numbRows;
  }

  /**
   * Print table rows
   * 
   * Print the first number of specified table rows
   * 
   */
  public static void printRows(String pathTable1, long numbRows)
      throws IOException {
    System.out.println("printRows()");

    // Load table1
    String query1 = "table1 = LOAD '" + pathTable1
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query1);
    
   //
    // Get metrics from first table (unsorted)
    //
    long count = 0;
    Iterator<Tuple> it1 = pigServer.openIterator("table1");

    while (it1.hasNext()) {
      ++count;
      if (count > numbRows)
        break;
      Tuple RowValue1 = it1.next();
      System.out.println();
      for (int i = 0; i < RowValue1.size(); ++i)
        System.out.println("DEBUG: " + "table" + " RowValue.get(" + i + ") = "
            + RowValue1.get(i));
    }
    System.out.println("\nTable Path : " + pathTable1);
    System.out.println("Table Rows Printed : " + numbRows);
  }
  
  /* 
  * Print the first number of specified table rows
  * 
  */
 public static void printRowNumber(String pathTable1, String sortKey)
      throws IOException, ParseException {
    long numbRows = 0;

    Path tablePath = new Path(pathTable1);

    BasicTable.Reader reader = new BasicTable.Reader(tablePath, conf);
   
    JobConf conf1 = new JobConf(conf);
    System.out.println("sortKey: " + sortKey);
    TableInputFormat.setInputPaths(conf1, new Path(pathTable1));

    TableInputFormat.requireSortedTable(conf1, null);
    TableInputFormat tif = new TableInputFormat();

  
    TableScanner scanner = reader.getScanner(null, null, true);
    BytesWritable key = new BytesWritable();
    Tuple rowValue = TypesUtils.createTuple(scanner.getSchema());

    while (!scanner.atEnd()) {
      ++numbRows;
      scanner.getKey(key);
      scanner.advance();
    }
    System.out.println("\nTable Path : " + pathTable1);
    System.out.println("Table Row number : " + numbRows);
  }
  /**
   * Compare table rows
   * 
   */
  private static boolean compareRow(Tuple rowValues1, Tuple rowValues2)
      throws IOException {
    boolean result = true;
    Assert.assertEquals(rowValues1.size(), rowValues2.size());
    for (int i = 0; i < rowValues1.size(); ++i) {
      if (!compareObj(rowValues1.get(i), rowValues2.get(i))) {
        System.out.println("DEBUG: " + " RowValue.get(" + i
            + ") value compare error : " + rowValues1.get(i) + " : "
            + rowValues2.get(i));
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
  private static boolean compareObj(Object object1, Object object2) {
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
   * Compares two objects that implement the Comparable interface
   * 
   * Zebra supported "sort" types of String, DataByteArray, Integer, Float,
   * Long, Double, and Boolean all implement the Comparable interface.
   * 
   * Returns a negative integer, zero, or a positive integer if object1 is less
   * than, equal to, or greater than object2.
   * 
   */
  private static int compareTo(Object object1, Object object2) {
    if (object1 == null) {
      if (object2 == null)
        return 0;
      else
        return -1;
    } else if (object2 == null) {
      return 1;
    } else
      return ((Comparable) object1).compareTo((Comparable) object2);
  }

  /**
   * Print Table Metadata Info (for debugging)
   * 
   */
  private static void printTableInfo(String pathString) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo ===========");
    BasicTable.dumpInfo(pathString, ps, conf);

    System.out.println("bos.toString() : " + bos.toString());
  }

  /**
   * Print Pig Table (for debugging)
   * 
   */
  private static int printTable(String tablename) throws IOException {
    Iterator<Tuple> it1 = pigServer.openIterator(tablename);
    int numbRows = 0;
    while (it1.hasNext()) {
      Tuple RowValue1 = it1.next();
      ++numbRows;
      System.out.println();
      for (int i = 0; i < RowValue1.size(); ++i)
        System.out.println("DEBUG: " + tablename + " RowValue.get(" + i
            + ") = " + RowValue1.get(i));
    }
    System.out.println("\nRow count : " + numbRows);
    return numbRows;
  }

  /**
   * Calculate elapsed time
   * 
   */
  private static String printTime(long start, long stop) {
    long timeMillis = stop - start;
    long time = timeMillis / 1000;
    String seconds = Integer.toString((int) (time % 60));
    String minutes = Integer.toString((int) ((time % 3600) / 60));
    String hours = Integer.toString((int) (time / 3600));

    for (int i = 0; i < 2; i++) {
      if (seconds.length() < 2) {
        seconds = "0" + seconds;
      }
      if (minutes.length() < 2) {
        minutes = "0" + minutes;
      }
      if (hours.length() < 2) {
        hours = "0" + hours;
      }
    }
    String formatTime = hours + ":" + minutes + ":" + seconds;
    return formatTime;
  }

  /**
   * Main
   * 
   * Command line options:
   * 
   * -verifyOption : <load, sort, merge-join, sorted-union, dump, tableinfo,
   * createtable, createsorttable, deletetable, printrows>
   * 
   * -pathTable1 : <hdfs path> -pathTable2 : <hdfs path>
   * 
   * -pathUnionTables : <hdfs path> <hdfs path> ...
   * 
   * -rowMod : verify every nth row (optional)
   * 
   * -numbCols : number of columns table should have (optional)
   * 
   * -sortCol : for sort option (default is column 0)
   * 
   * -sortString : sort string for sort option
   * 
   * -numbRows : number of rows for new table to create
   * 
   * -seed : unique column number used for creating new tables
   * 
   * -debug : print out debug info with results (use caution, for example do not
   * used when creating large tables)
   * 
   * examples:
   * 
   * java -DwhichCluster="realCluster" -DHADOOP_HOME=$HADOOP_HOME -DUSER=$USER
   * TestComparator -verifyOption load -pathTable1 /user/hadoopqa/table1
   * -pathTable2 /user/hadoopqa/table2
   * 
   * java -DwhichCluster="realCluster" -DHADOOP_HOME=$HADOOP_HOME -DUSER=$USER
   * TestComparator -verifyOption sort -pathTable1 /user/hadoopqa/table1
   * -pathTable2 /user/hadoopqa/table2 -sortCol 0
   * 
   * java -DwhichCluster="realCluster" -DHADOOP_HOME=$HADOOP_HOME -DUSER=$USER
   * TestComparator -verifyOption merge-join -pathTable1 /user/hadoopqa/table1
   * -sortCol 0
   * 
   * java -DwhichCluster="realCluster" -DHADOOP_HOME=$HADOOP_HOME -DUSER=$USER
   * TestComparator -verifyOption sorted-union -pathTable1
   * /user/hadoopqa/unionTable1 -pathUnionTables /user/hadoopqa/inputTable1
   * /user/hadoopqa/inputTable2 /user/hadoopqa/inputTable3 -sortCol 0 -rowMod 5
   * 
   * java -DwhichCluster="realCluster" -DHADOOP_HOME=$HADOOP_HOME -DUSER=$USER
   * TestComparator -verifyOption dump -pathTable1 /user/hadoopqa/table1
   * 
   * @param args
   */

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();

    System.out.println("Running Zebra TestComparator");
    try {
      ArrayList<String> unionPaths = new ArrayList<String>();
      String verifyOption = null;
      String pathTable1 = null;
      String pathTable2 = null;
      String sortString = null;
      String verifyDataColName = null;
      int rowMod = 1; // default to verify every table row
      int numbCols = 0; // if provided, verify that table has these number of
      // columns
      int sortCol = 0; // default to first column as sort index
      long numbRows = 0; // number of rows to create for new table
      int seed = 0; // used for creating new tabletable1
      boolean debug = false;

      // Read arguments
      if (args.length >= 2) {
        for (int i = 0; i < args.length; ++i) {

          if (args[i].equals("-verifyOption")) {
            verifyOption = args[++i];
          } else if (args[i].equals("-pathTable1")) {
            pathTable1 = args[++i];
          } else if (args[i].equals("-pathTable2")) {
            pathTable2 = args[++i];
          } else if (args[i].equals("-pathUnionTables")) {
            while (++i < args.length && !args[i].startsWith("-")) {
              System.out.println("args[i] : " + args[i]);
              unionPaths.add(args[i]);
            }
            if (i < args.length)
              --i;
          } else if (args[i].equals("-rowMod")) {
            rowMod = new Integer(args[++i]).intValue();
          } else if (args[i].equals("-sortString")) {
            sortString = args[++i];
          } else if (args[i].equals("-sortCol")) {
            sortCol = new Integer(args[++i]).intValue();
          } else if (args[i].equals("-numbCols")) {
            numbCols = new Integer(args[++i]).intValue();
          } else if (args[i].equals("-numbRows")) {
            numbRows = new Long(args[++i]).intValue();
          } else if (args[i].equals("-seed")) {
            seed = new Integer(args[++i]).intValue();
          } else if (args[i].equals("-verifyDataColName")) {
            verifyDataColName = args[++i];
          } else if (args[i].equals("-debug")) {
            debug = true;
          } else {
            System.out.println("Exiting - unknown argument : " + args[i]);
            System.exit(0);
          }
        }
      } else {
        System.out
            .println("Error - need to provide required comparator arguments");
        System.exit(0);
      }

      // Setup environment
      setUp();

      //
      // Run appropriate verify option
      //
      if (verifyOption == null) {
        System.out.println("Exiting -verifyOption not set");
        System.exit(0);
      }

      if (verifyOption.equals("load")) {
        // Verify both tables are equal
        verifyLoad(pathTable1, pathTable2, numbCols);
      } else if (verifyOption.equals("sort")) {
        // Verify table is in sorted order
        verifySorted(pathTable1, pathTable2, sortCol, sortString, numbCols,
            rowMod);
      } else if (verifyOption.equals("merge-join")) {
        // Verify merge-join table is in sorted order
        verifyMergeJoin(pathTable1, sortCol, sortString, numbCols, rowMod,verifyDataColName);
      } else if (verifyOption.equals("sorted-union")) {
        Object lastVal = null;
        
        // Verify sorted-union table is in sorted order
        verifySortedUnion(unionPaths, pathTable1, sortCol, sortString,
            numbCols, rowMod, verifyDataColName);
      } else if (verifyOption.equals("dump")) {
        // Dump table info
        printTableInfo(pathTable1);
      } else if (verifyOption.equals("tableinfo")) {
        // Verify table to get row and column info
        verifyTable(pathTable1);
      } else if (verifyOption.equals("deletetable")) {
        // Delete table directory
        deleteTable(pathTable1);
      } else if (verifyOption.equals("printrows")) {
        // Print some table rows
        printRows(pathTable1, numbRows);
      } else if (verifyOption.equals("createtable")) {
        // Create unsorted table
        createtable(pathTable1, numbRows, seed, debug);
      } else if (verifyOption.equals("createsorttable")) {
        // Create sorted table
        createsortedtable(pathTable1, pathTable2, sortString, debug);
      }else if (verifyOption.equals("printrownumber")) {
        Object lastVal = null;
        //print total number of rows of the table
        printRowNumber(pathTable1,sortString);
      }
      //
      else {
        System.out.println("Exiting - unknown -verifyOption value : "
            + verifyOption);
        System.exit(0);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    long stopTime = System.currentTimeMillis();
    System.out.println("\nElapsed time : " + printTime(startTime, stopTime)
        + "\n");
  }

}
