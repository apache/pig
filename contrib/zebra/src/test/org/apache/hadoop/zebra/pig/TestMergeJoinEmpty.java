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

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestMergeJoinEmpty extends BaseTestCase
{

  final static int numsBatch = 4;
  final static int numsInserters = 1;

  static Path pathTable1;
  static Path pathTable2;
  final static String STR_SCHEMA1 = "a:int,b:float,c:long,d:double,e:string,f:bytes,r1:record(f1:string, f2:string),m1:map(string)";
  final static String STR_SCHEMA2 = "m1:map(string),r1:record(f1:string, f2:string),f:bytes,e:string,d:double,c:long,b:float,a:int";

  final static String STR_STORAGE1 = "[a, b, c]; [e, f]; [r1.f1]; [m1#{a}]";
  final static String STR_STORAGE2 = "[a];[b]; [c]; [e]; [f]; [r1.f1]; [m1#{a}]";
  static int t1 =0;
  

  @BeforeClass
  public static void setUp() throws Exception {
    init();
    pathTable1 = getTableFullPath("TestMergeJoinEmpty1") ;
    pathTable2 = getTableFullPath("TestMergeJoinEmpty2") ;
    removeDir(pathTable1);
    removeDir(pathTable2);
    createFirstTable();
    createSecondTable();    
    
  }
  
  public static void createFirstTable() throws IOException, ParseException {
    BasicTable.Writer writer = new BasicTable.Writer(pathTable1, STR_SCHEMA1,
        STR_STORAGE1, conf);
    Schema schema = writer.getSchema();
    //System.out.println("typeName" + schema.getColumn("a").type.pigDataType());
    Tuple tuple = TypesUtils.createTuple(schema);
  
    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }
    Tuple tupRecord1;
    try {
      tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    Map<String, String> m1 = new HashMap<String, String>();
    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tupRecord1);
        TypesUtils.resetTuple(tuple);
        m1.clear();
  
        try {
          // first row of the table , the biggest row
          if (i == 0 && b == 0) {
            tuple.set(0, 100);
            tuple.set(1, 100.1f);
            tuple.set(2, 100L);
            tuple.set(3, 50e+2);
            tuple.set(4, "something");
            tuple.set(5, new DataByteArray("something"));
  
          }
          // the middle + 1 row of the table, the smallest row
          else if (i == 0 && b == (numsBatch / 2)) {
            tuple.set(0, -100);
            tuple.set(1, -100.1f);
            tuple.set(2, -100L);
            tuple.set(3, -50e+2);
            tuple.set(4, "so");
            tuple.set(5, new DataByteArray("so"));
  
          }
  
          else {
            Float f = 1.1f;
            long l = 11;
            double d = 1.1;
            tuple.set(0, b);
            tuple.set(1, f);
            tuple.set(2, l);
            tuple.set(3, d);
            tuple.set(4, "some");
            tuple.set(5, new DataByteArray("some"));
          }
  
          // insert record
          tupRecord1.set(0, "" + b);
          tupRecord1.set(1, "" + b);
          tuple.set(6, tupRecord1);
  
          // insert map
          m1.put("a", "" + b);
          m1.put("b", "" + b);
          m1.put("c", "" + b);
          tuple.set(7, m1);
  
        } catch (ExecException e) {
          e.printStackTrace();
        }
  
       inserters[i].insert(new BytesWritable(("key_" + b).getBytes()), tuple);
       
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
  
    //check table is setup correctly
    String projection = new String("a,b,c,d,e,f,r1,m1");
    
    BasicTable.Reader reader = new BasicTable.Reader(pathTable1, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());
  
    scanner.getValue(RowValue);
    System.out.println("rowvalue size:"+RowValue.size());
    System.out.println("read a : " + RowValue.get(0).toString());
    System.out.println("read string: " + RowValue.get(1).toString());
  
    scanner.advance();
    scanner.getValue(RowValue);
    System.out.println("read float in 2nd row: "+ RowValue.get(1).toString());
    System.out.println("done insert table");
  
    reader.close();
    
  }
  
  public static void createSecondTable() throws IOException, ParseException {
    BasicTable.Writer writer = new BasicTable.Writer(pathTable2, STR_SCHEMA2,
        STR_STORAGE2, conf);
    Schema schema = writer.getSchema();
    //System.out.println("typeName" + schema.getColumn("a").type.pigDataType());
    Tuple tuple = TypesUtils.createTuple(schema);

    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }
    Tuple tupRecord1;
    try {
      tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    Map<String, String> m1 = new HashMap<String, String>();
    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tupRecord1);
        TypesUtils.resetTuple(tuple);
        m1.clear();

        try {
           // first row of the table , the biggest row
          if (i == 0 && b == 0) {
            tuple.set(7, 100);
            tuple.set(6, 100.1f);
            tuple.set(5, 100L);
            tuple.set(4, 50e+2);
            tuple.set(3, "something");
            tuple.set(2, new DataByteArray("something"));

          }
          // the middle +1 row of the table, the smallest row
          else if (i == 0 && b == (numsBatch / 2)) {
            tuple.set(7, -100);
            tuple.set(6, -100.1f);
            tuple.set(5, -100L);
            tuple.set(4, -50e+2);
            tuple.set(3, "so");
            tuple.set(2, new DataByteArray("so"));

          }

          else {
            Float f = 2.1f;
            long l = 12;
            double d = 2.1;
            tuple.set(7, b*2);
            tuple.set(6, f);
            tuple.set(5, l);
            tuple.set(4, d);
            tuple.set(3, "somee");
            tuple.set(2, new DataByteArray("somee"));
          }

          // insert record
          tupRecord1.set(0, "" + b);
          tupRecord1.set(1, "" + b);
          tuple.set(1, tupRecord1);

          // insert map

          m1.put("a", "" + b);
          m1.put("b", "" + b);
          m1.put("c", "" + b);
          tuple.set(0, m1);

        } catch (ExecException e) {
          e.printStackTrace();
        }
//      empty table
//        inserters[i].insert(new BytesWritable(("key" + b).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
 
    //check table is setup correctly
    String projection = new String("a,b,c,d,e,f,r1,m1");
    
    BasicTable.Reader reader = new BasicTable.Reader(pathTable2, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    /*
    scanner.getValue(RowValue);
    System.out.println("rowvalue size:"+RowValue.size());
    System.out.println("read a : " + RowValue.get(7).toString());
    System.out.println("read string: " + RowValue.get(6).toString());
   
    scanner.advance();
    scanner.getValue(RowValue);
    System.out.println("read float in 2nd row: "+ RowValue.get(6).toString());
    System.out.println("done insert table");
    */

    reader.close();
    
  }
  
  public static void sortTable(Path tablePath, String sortkey){
    
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    pigServer.shutdown();
  }


  public void verify(Iterator<Tuple> it3) throws ExecException {
    int row = 0;
    Tuple RowValue3 = null;
    Assert.assertEquals(0, row);
  }

  public Iterator<Tuple> joinTable(String table1, String table2, String sortkey1, String sortkey2) throws IOException {
    String query1 = "records1 = LOAD '" + this.pathTable1.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("query1:" + query1);
    pigServer.registerQuery(query1);

    String query2 = "records2 = LOAD '" + this.pathTable2.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("query2:" + query2);
    pigServer.registerQuery(query2);

   /* Iterator<Tuple> it_before_order = pigServer.openIterator("records");
    int row_before_order = 0;
    Tuple RowValue_before_order = null;
    while (it_before_order.hasNext()) {
      RowValue_before_order = it_before_order.next();
      row_before_order++;
      System.out.println("row : " + row_before_order + " field f value: "
          + RowValue_before_order.get(5));
    }
    System.out.println("total row for orig table before ordered:"
        + row_before_order);*/
    String orderby1 = "sort1 = ORDER records1 BY " + sortkey1 + " ;";
    String orderby2 = "sort2 = ORDER records2 BY " + sortkey2 + " ;";
    pigServer.registerQuery(orderby1);
    pigServer.registerQuery(orderby2);

    /*Iterator<Tuple> it_after_order = pigServer.openIterator("srecs");
    int row_after_order = 0;
    Tuple RowValue_after_order = null;
    while (it_after_order.hasNext()) {
      RowValue_after_order = it_after_order.next();
      row_after_order++;
      System.out.println("row : " + row_after_order + " field b value: "
          + RowValue_after_order.get(1));
    }
    System.out.println("total row for orig table after ordered:"
        + row_after_order);*/
    // Path newPath = new Path(getCurrentMethodName());

    /*
     * Table1 creation
     */
    this.t1++;
    
    String table1path = this.pathTable1.toString() + Integer.toString(this.t1);
    pigServer.store("sort1", table1path, TableStorer.class.getCanonicalName()
        + "('[a, b, c]; [d, e, f, r1, m1]')");

    String query3 = "records1 = LOAD '"
        + table1path
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, r1, m1', 'sorted');";

    System.out.println("query3:" + query3);
    pigServer.registerQuery(query3);   
    
    String foreach = "records11 = foreach records1 generate a as a, b as b, c as c, d as d, e as e, f as f, r1 as r1, m1 as m1;";
    pigServer.registerQuery(foreach);
   /* Iterator<Tuple> it_ordered = pigServer.openIterator("records1");
    int row_ordered = 0;
    Tuple RowValue_ordered = null;
    while (it_ordered.hasNext()) {
      RowValue_ordered = it_ordered.next();
      row_ordered++;
      System.out.println("row : " + row_ordered + " field a value: "
          + RowValue_ordered.get(0));
    }
    System.out.println("total row for table 1 after ordered:" + row_ordered);*/

    /*
     * Table2 creation
     */
    this.t1++;
    String table2path = this.pathTable2.toString() + Integer.toString(this.t1);
    pigServer.store("sort2", table2path, TableStorer.class.getCanonicalName()
        + "('[a, b, c]; [d,e,f,r1,m1]')");

    String query4 = "records2 = LOAD '" + table2path
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query4);

    
    String filter = "records22 = FILTER records2 BY a == '1.9';";
    pigServer.registerQuery(filter);
    /*Iterator<Tuple> it_ordered2 = pigServer.openIterator("records2");
    int row_ordered2 = 0;
    Tuple RowValue_ordered2 = null;
    while (it_ordered2.hasNext()) {
      RowValue_ordered2 = it_ordered2.next();
      row_ordered2++;
      System.out.println("row for table 2 after ordereed: " + row_ordered2
          + " field a value: " + RowValue_ordered2.get(0));
    }

    System.out.println("total row for table 2:" + row_ordered2);
    */
    String join = "joinRecords = JOIN records11 BY " + "(" + sortkey1 + ")" 
        + " , records2 BY " + "("+ sortkey2 + ")"+" USING \"merge\";";
   //TODO: can not use records22
      pigServer.registerQuery(join);
   
    // check JOIN content
    Iterator<Tuple> it3 = pigServer.openIterator("joinRecords");
    return it3;
  }
  
  @Test
  public void test1() throws ExecException, IOException {
    /*
     * join key: single byte column
     */
    /*
     * right empty table
     */
    Iterator<Tuple> it3 = joinTable(this.pathTable1.toString(), this.pathTable2.toString(), "f","f" );
    verify(it3);
  }
  
  @Test
  public void test2() throws ExecException, IOException {
    /*
     * join key: single byte column
     */
    /*
     * left empty table
     */
    Iterator<Tuple> it3 = joinTable(this.pathTable2.toString(), this.pathTable1.toString(), "f","f" );
    verify(it3);
  }
  
}
