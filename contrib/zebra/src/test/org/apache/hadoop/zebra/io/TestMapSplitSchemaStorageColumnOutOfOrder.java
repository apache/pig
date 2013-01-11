/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownershcolumn3. The ASF
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
package org.apache.hadoop.zebra.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test projections on complicated column types: Jira 1026
 * 
 */
public class TestMapSplitSchemaStorageColumnOutOfOrder {
  final static String STR_SCHEMA = "column1:bytes,column2:bytes, column3:bytes,column4:bytes,column5:map(String),column6:map(String),column7:map(String),column8:collection(record(f1:map(String)))";   
  final static String STR_STORAGE = "[column1,column2,column3,column4];[column5#{key51|key52|key53|key54|key55|key56},column7#{key71|key72|key73|key74|key75}];[column5,column7,column6];[column8]";
  private static Configuration conf;
  private static Path path;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    conf = new Configuration();
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");

    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestMapSplitSchemaStorageColumnOutOfOrder");
    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);
    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);
    writer.finish();
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    DataBag bag2 = TypesUtils.createBag();
    Schema schColl2 = schema.getColumn(7).getSchema();
    Tuple tupColl2_1 = TypesUtils.createTuple(schColl2);
    Tuple tupColl2_2 = TypesUtils.createTuple(schColl2);
    // add data to row 1
    tuple.set(0, new DataByteArray("column1 row 1 ")); // byte
    tuple.set(1,new DataByteArray("column2 row 1"));
    tuple.set(2, new DataByteArray("column3 row 1"));
    tuple.set(3, new DataByteArray("column4 row 1"));
    
    // column5
    Map<String, String> column5 = new HashMap<String, String>();
    column5.put("key51", "key511");
    column5.put("key52", "key521");
    column5.put("key53", "key531");
    column5.put("key54", "key541");
    column5.put("key55", "key551");
    column5.put("key56", "key561");
    column5.put("key57", "key571");
   
    tuple.set(4, column5);

    //column5:map(bytes),column6:map(bytes),column7:map(bytes),column8:collection(f1:map(bytes)
    //column7:map(String, column6:map(String)
    HashMap<String, String> column7 = new HashMap<String, String>();
    HashMap<String, String> column6 = new HashMap<String, String>();
    column6.put("column61", "column61");
    column7.put("key71", "key711");
    column7.put("key72", "key721");
    column7.put("key73", "key731");
    column7.put("key74", "key741");
    column7.put("key75", "key751");
    tuple.set(6, column7);
    tuple.set(5, column6);
    
    //column8:collection(f1:map(bytes))
    HashMap<String, String> mapInCollection = new HashMap<String, String>();
    mapInCollection.put("mc", "mc1");
    tupColl2_1.set(0, mapInCollection);
    bag2.add(tupColl2_1);

    tupColl2_2.set(0, mapInCollection);
    bag2.add(tupColl2_2);
    tuple.set(7, bag2);

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    column5.clear();
    column7.clear();
    column6.clear();
    mapInCollection.clear();
    bag2.clear();
    TypesUtils.resetTuple(tupColl2_1);
    TypesUtils.resetTuple(tupColl2_2);
    
    tuple.set(0, new DataByteArray("column1 row 2 ")); // byte
    tuple.set(1,new DataByteArray("column2 row 2"));
    tuple.set(2, new DataByteArray("column3 row 2"));
    tuple.set(3, new DataByteArray("column4 row 2"));
    
    // column5
    column5.put("key51", "key512");
    column5.put("key52", "key522");
    column5.put("key53", "key532");
    column5.put("key54", "key542");
    column5.put("key55", "key552");
    column5.put("key56", "key562");
    column5.put("key57", "key572");
    tuple.set(4, column5);

    // column6
  
    column6.put("column6", "column62");
    column7.put("key71", "key712");
    column7.put("key72", "key722");
    column7.put("key73", "key732");
    column7.put("key74", "key742");
    column7.put("key75", "key752");
    tuple.set(6, column7);
    tuple.set(5, column6);
    
    
    //column8
    //column8:collection(f1:map(bytes))
    mapInCollection.put("mc", "mc2");
    tupColl2_1.set(0, mapInCollection);
    bag2.add(tupColl2_1);

    tupColl2_2.set(0, mapInCollection);
    bag2.add(tupColl2_2);
    tuple.set(7, bag2);

    
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // finish building table, closing out the inserter, writer, writer1
    inserter.close();
    writer1.finish();
    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
    BasicTable.drop(path, conf);
  }

 
  @Test
  public void testRead1() throws IOException, ParseException {
    /*
     * read one map
     *  column5.put("key51", "key511");
     */
    String projection = new String("column5#{key51}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("read1 : " + RowValue.toString());
    Assert.assertEquals("{key51=key511}", RowValue.get(0).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(0).toString());
    Assert.assertEquals("{key51=key512}", RowValue.get(0).toString());

    reader.close();
  }

 
  @Test
  public void testRead2() throws IOException, ParseException {
    /*
     * read map , stitch
     * [column5#{key51|key52|key53|key54|key55|key56},column7#{key71|key72|key73|key74|key75}];[column5,column7,column6]
     */
    String projection2 = new String("column5#{new}, column7#{key71|ytestid}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
//    reader.setProjection(projection2);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("map of map: " + RowValue.toString());
    Assert.assertEquals("key571", ((Map) RowValue.get(4)).get("key57"));
    Assert.assertEquals("key711", ((Map) RowValue.get(6)).get("key71"));
       
    Assert.assertEquals(null, (((Map) RowValue.get(6)).get("ytestid")));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(6)).get("x")));
    System.out.println("rowValue.get)1): " + RowValue.get(6).toString());
    // rowValue.get)1): {z=null, x={m311=311, m321=321, m331=331}}

    scanner.advance();

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals("key572", ((Map) RowValue.get(4)).get("key57"));
    Assert.assertEquals("key712", ((Map) RowValue.get(6)).get("key71"));
       
    Assert.assertEquals("key722", ((Map) RowValue.get(6)).get("key72"));
    Assert.assertEquals(null, ((Map) ((Map) RowValue.get(6)).get("x")));
    reader.close();

  }

  @Test
  public void testRead3() throws IOException, ParseException {
    /*
     * negative , read one map who is non-exist 
     */
    String projection = new String("m5");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);

    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    Assert.assertEquals(null, RowValue.get(0));
    Assert.assertEquals(1, RowValue.size());
    reader.close();
  }

  @Test
  public void testRead4() throws IOException, ParseException {
    /*
     *  Not exist key for all rows
     */
    String projection = new String("column5#{nonexist}");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    System.out.println("read1 : " + RowValue.toString());
    Assert.assertEquals("{}", RowValue.get(0).toString());

    scanner.advance();
    scanner.getKey(key);
    Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    System.out.println(RowValue.get(0).toString());
    Assert.assertEquals("{}", RowValue.get(0).toString());

    reader.close();
  }
}
