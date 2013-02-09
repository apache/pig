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
package org.apache.hadoop.zebra.pig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.StringTokenizer;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.executionengine.ExecJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test projections on complicated column types.
 * 
 */
public class TestSimpleType extends BaseTestCase {

  final static String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:double, s5:string, s6:bytes";
  final static String STR_STORAGE = "[s1, s2]; [s3, s4]; [s5, s6]";
  private static Path path;

  @BeforeClass
  public static void setUpOnce() throws IOException, Exception {
    init();


    path = getTableFullPath("TesMapType");
    removeDir(path);

    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);
    Schema schema = writer.getSchema();

    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);

    Tuple tuple = TypesUtils.createTuple(schema);
    TypesUtils.resetTuple(tuple);

    // insert data in row 1
    int row = 0;
    tuple.set(0, true); // bool
    tuple.set(1, 1); // int
    tuple.set(2, 1001L); // long
    tuple.set(3, 1.1); // float
    tuple.set(4, "hello world 1"); // string
    tuple.set(5, new DataByteArray("hello byte 1")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    row++;
    TypesUtils.resetTuple(tuple);

    tuple.set(0, false);
    tuple.set(1, 2); // int
    tuple.set(2, 1002L); // long
    tuple.set(3, 3.1); // float
    tuple.set(4, "hello world 2"); // string
    tuple.set(5, new DataByteArray("hello byte 2")); // byte
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    inserter.close();
    writer1.finish();
    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
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
  
  // @Test
  public void testReadSimpleStitch() throws IOException, ParseException {
    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s5,s1');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    BytesWritable key = new BytesWritable();
    int row = 0;
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      row++;
      if (row == 1) {
        Assert.assertEquals("hello world 1", RowValue.get(0));
        Assert.assertEquals(true, RowValue.get(1));
      }
      if (row == 2) {
        Assert.assertEquals("hello world 2", RowValue.get(0));
        Assert.assertEquals(false, RowValue.get(1));
      }
    }
  }

  // @Test
  // Test reader
  public void testReadSimple1() throws IOException, ParseException {
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s6,s5,s4,s3,s2,s1');";
    System.out.println(query);
    pigServer.registerQuery(query);
    Iterator<Tuple> it = pigServer.openIterator("records");
    BytesWritable key = new BytesWritable();
    int row = 0;
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
      row++;
      if (row == 1) {
        // Assert.assertEquals(key, new
        // BytesWritable("k11".getBytes()));
        Assert.assertEquals(true, RowValue.get(5));
        Assert.assertEquals(1, RowValue.get(4));
        Assert.assertEquals(1001L, RowValue.get(3));
        Assert.assertEquals(1.1, RowValue.get(2));
        Assert.assertEquals("hello world 1", RowValue.get(1));
        Assert.assertEquals("hello byte 1", RowValue.get(0).toString());
      }
      if (row == 2) {
        Assert.assertEquals(false, RowValue.get(5));
        Assert.assertEquals(2, RowValue.get(4));
        Assert.assertEquals(1002L, RowValue.get(3));
        Assert.assertEquals(3.1, RowValue.get(2));
        Assert.assertEquals("hello world 2", RowValue.get(1));
        Assert.assertEquals("hello byte 2", RowValue.get(0).toString());
      }
    }
  }

  // @Test
  // Test reader, negative. not exist field in the projection
  public void testRead2() throws IOException, ParseException {
    try {
      String query = "records = LOAD '" + path.toString()
          + "' USING org.apache.hadoop.zebra.pig.TableLoader('s7');";
      Assert.fail("Project should not take non-existent fields");
    } catch (Exception e) {
      System.out.println(e);
    }

  }

  @Test
  // Store same table
  public void testStorer() throws ExecException, IOException {
    //
    // Use pig LOAD to load testing data for store
    //
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);";
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
    }

    //
    // Use pig STORE to store testing data
    //
    Path newPath = new Path(getCurrentMethodName());
    pigServer
        .store(
            "records",
            new Path(newPath, "store").toString(),
            TableStorer.class.getCanonicalName()
                + "('[s1, s2]; [s3, s4]')");

  }

  @Test
  // store different records, second row of the previous table
  public void testStorer2() throws ExecException, IOException {
    // Load original table
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);";
    System.out.println(query);
    pigServer.registerQuery(query);

    // filter the original table
    String query2 = "newRecord = FILTER records BY (s2 >= 2);";
    pigServer.registerQuery(query2);

    // store the new records to new table
    Path newPath = new Path(getCurrentMethodName());
    pigServer
        .store(
            "newRecord",
            newPath.toString(),
            TableStorer.class.getCanonicalName()
                + "('[s1, s2]; [s3, s4]')");

    // check new table content
    String query3 = "newRecords = LOAD '"
        + newPath.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s6,s5,s4,s3,s2,s1');";
    System.out.println(query3);
    // newRecords = LOAD
    // 'org.apache.hadoop.zebra.pig.TestSimpleType.testStorer2' USING
    // org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);
    pigServer.registerQuery(query3);

    Iterator<Tuple> it3 = pigServer.openIterator("newRecords");
    // BytesWritable key2 = new BytesWritable();
    int row = 0;
    Tuple RowValue2 = null;
    while (it3.hasNext()) {
      // Last row value
      RowValue2 = it3.next();
      row++;
      if (row == 1) {
        Assert.assertEquals(false, RowValue2.get(5));
        Assert.assertEquals(2, RowValue2.get(4));
        Assert.assertEquals(1002L, RowValue2.get(3));
        Assert.assertEquals(3.1, RowValue2.get(2));
        Assert.assertEquals("hello world 2", RowValue2.get(1));
        Assert.assertEquals("hello byte 2", RowValue2.get(0).toString());
      }
    }
    Assert.assertEquals(1, row);
  }

  @Test
  // store different records, with storage hint is empty
  public void testStorer3() throws ExecException, IOException {
    // Load original table
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);";
    System.out.println(query);
    pigServer.registerQuery(query);

    // filter the original table
    String query2 = "newRecord = FILTER records BY (s2 >= 2);";
    pigServer.registerQuery(query2);

    // store the new records to new table
    Path newPath = new Path(getCurrentMethodName());
    pigServer.store("newRecord", newPath.toString(), TableStorer.class
        .getCanonicalName()
        + "('')");

    // check new table content
    String query3 = "newRecords = LOAD '"
        + newPath.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s6,s5,s4,s3,s2,s1');";
    System.out.println(query3);
    // newRecords = LOAD
    // 'org.apache.hadoop.zebra.pig.TestSimpleType.testStorer2' USING
    // org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);
    pigServer.registerQuery(query3);

    Iterator<Tuple> it3 = pigServer.openIterator("newRecords");
    // BytesWritable key2 = new BytesWritable();
    int row = 0;
    Tuple RowValue2 = null;
    while (it3.hasNext()) {
      // Last row value
      RowValue2 = it3.next();
      row++;
      if (row == 1) {
        Assert.assertEquals(false, RowValue2.get(5));
        Assert.assertEquals(2, RowValue2.get(4));
        Assert.assertEquals(1002L, RowValue2.get(3));
        Assert.assertEquals(3.1, RowValue2.get(2));
        Assert.assertEquals("hello world 2", RowValue2.get(1));
        Assert.assertEquals("hello byte 2", RowValue2.get(0).toString());
      }
    }
    Assert.assertEquals(1, row);
  }

  @Test
  // store different records, with column group is empty
  public void testStorer4() throws ExecException, IOException {
    // Load original table
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);";
    System.out.println(query);
    pigServer.registerQuery(query);

    // filter the original table
    String query2 = "newRecord = FILTER records BY (s2 >= 2);";
    pigServer.registerQuery(query2);

    // store the new records to new table
    Path newPath = new Path(getCurrentMethodName());
    pigServer.store("newRecord", newPath.toString(), TableStorer.class
        .getCanonicalName()
        + "('[]')");

    // check new table content
    String query3 = "newRecords = LOAD '"
        + newPath.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('s6,s5,s4,s3,s2,s1');";
    System.out.println(query3);
    // newRecords = LOAD
    // 'org.apache.hadoop.zebra.pig.TestSimpleType.testStorer2' USING
    // org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);
    pigServer.registerQuery(query3);

    Iterator<Tuple> it3 = pigServer.openIterator("newRecords");
    // BytesWritable key2 = new BytesWritable();
    int row = 0;
    Tuple RowValue2 = null;
    while (it3.hasNext()) {
      // Last row value
      RowValue2 = it3.next();
      row++;
      if (row == 1) {
        Assert.assertEquals(false, RowValue2.get(5));
        Assert.assertEquals(2, RowValue2.get(4));
        Assert.assertEquals(1002L, RowValue2.get(3));
        Assert.assertEquals(3.1, RowValue2.get(2));
        Assert.assertEquals("hello world 2", RowValue2.get(1));
        Assert.assertEquals("hello byte 2", RowValue2.get(0).toString());
      }
    }
    Assert.assertEquals(1, row);
  }

  @Test
  // negative, schema description is different from input tuple, less column
  // numbers
  public void testStorerNegative1() throws ExecException, IOException {

    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
    }

    try {
        Path newPath = new Path(getCurrentMethodName());
        ExecJob pigJob = pigServer
            .store(
                "records",
                new Path(newPath, "store").toString(),
                TableStorer.class.getCanonicalName()
                    + "('[s7, s2]; [s3, s4]')");
    } catch (Exception e) {
        System.out.println(e);
        return;
    }
    Assert.fail("Exception expected");
  }

  @Test
  // negative, storage hint duplicate the columns
  public void testStorerNegative2() throws ExecException, IOException {

    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
    }

    try {
        Path newPath = new Path(getCurrentMethodName());
        
        ExecJob pigJob = pigServer
              .store(
                  "records",
                  new Path(newPath, "store").toString(),
                  TableStorer.class.getCanonicalName()
                      + "('[s1, s2]; [s1, s4]')");
    } catch(Exception e) {
        System.out.println(e);
        return;
    }
    Assert.fail("Exception expected");
  }

  @Test
  // negative, storage hint duplicate the column groups
  public void testStorerNegative3() throws ExecException, IOException {

    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
    }
    try{
        Path newPath = new Path(getCurrentMethodName());
    
        ExecJob pigJob = pigServer
            .store(
                "records",
                new Path(newPath, "store").toString(),
                TableStorer.class.getCanonicalName()
                    + "('[s1]; [s1]')");
    } catch(Exception e) {
        System.out.println(e);
        return;
    }
    Assert.fail("Exception expected");

  }

  // @Test
  // negative, schema description is different from input tuple, different
  // data types for columns
  public void testStorerNegative4() throws ExecException, IOException {

    String query = "records = LOAD '" + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query);

    Iterator<Tuple> it = pigServer.openIterator("records");
    while (it.hasNext()) {
      Tuple RowValue = it.next();
      System.out.println(RowValue);
    }

    Path newPath = new Path(getCurrentMethodName());
    ExecJob pigJob = pigServer
        .store(
            "records",
            new Path(newPath, "store").toString(),
            TableStorer.class.getCanonicalName()
                + "('[s1, s2]; [s3, s4]')");
    Assert.assertNotNull(pigJob.getException());
    System.out.println(pigJob.getException());
  }

  @Test
  // Store negative, store to same path. Store should fail
  public void testStorer5() throws ExecException, IOException {   
    //
    // Use pig LOAD to load testing data for store
    //
    String query = "records = LOAD '"
        + path.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader() as (s1,s2,s3,s4,s5,s6);";
    pigServer.registerQuery(query);

    //
    // Use pig STORE to store testing data
    //
    System.out.println("path = " + path);
    try {
        ExecJob pigJob = pigServer
            .store(
                "records",
                path.toString(),
                TableStorer.class.getCanonicalName()
                    + "('[s1, s2]; [s3, s4]')");
    } catch(Exception e) {
        System.out.println(e);
        return;
    }
    Assert.fail("Exception expected");
  }
}
