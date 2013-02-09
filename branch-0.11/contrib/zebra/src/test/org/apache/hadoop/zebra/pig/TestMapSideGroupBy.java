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
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMapSideGroupBy extends BaseTestCase {
  final static int numsBatch = 4;
  final static int numsInserters = 1;
  static Path pathTable1;
  final static String STR_SCHEMA1 = "a:int,b:float,c:long,d:double,e:string,f:bytes,r1:record(f1:string, f2:string),m1:map(string)";
  final static String STR_STORAGE1 = "[a, b, c]; [e, f]; [r1.f1]; [m1#{a}]";
  static int t1 =0;

  @BeforeClass
  public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestGroupBy");
    removeDir(pathTable1);

    createFirstTable();
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

  @AfterClass
  public static void tearDown() throws Exception {
    pigServer.shutdown();
    BasicTable.drop(pathTable1, conf);
  }
  
  public void verify(Iterator<Tuple> it3) throws ExecException {
    int row = 0;
    Tuple RowValue3 = null;
    while (it3.hasNext()) {
      RowValue3 = it3.next();
      Assert.assertEquals(2, RowValue3.size());
      row++;
      String key = (String) RowValue3.get(0);
      DataBag bag = (DataBag) RowValue3.get(1);
      Iterator<Tuple> it = bag.iterator();
      if (key.equals("so")) {
        Assert.assertEquals(1, bag.size());
        Tuple t = it.next();
        Assert.assertEquals(-100, t.get(0));
      } else if (key.equals("something")) {
        Assert.assertEquals(1, bag.size());
        Tuple t = it.next();
        Assert.assertEquals(100, t.get(0));
      } else if (key.equals("some")) {
        Assert.assertEquals(2, bag.size());
        Tuple t1 = it.next(), t2 = it.next();
        int i1 = (Integer) t1.get(0), i2 = (Integer) t2.get(0);
        Assert.assertTrue((i1 == 1 && i2 == 3) || (i1 == 3 || i1 ==1));
      } else {
        Assert.fail("Unexprected key: " + key);
      }
    }
    Assert.assertEquals(3, row);
  }

  public Iterator<Tuple> groupby(String table1, String sortkey1) throws IOException {
    String query1 = "records1 = LOAD '" + pathTable1.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    System.out.println("query1:" + query1);
    pigServer.registerQuery(query1);

    String orderby1 = "sort1 = ORDER records1 BY " + sortkey1 + " ;";
    pigServer.registerQuery(orderby1);

    t1++;
    
    String table1path = pathTable1.toString() + Integer.toString(t1);
    removeDir(new Path(table1path));
    ExecJob pigJob = pigServer.store("sort1", table1path, TableStorer.class.getCanonicalName()
        + "('[a, b, c]; [d, e, f, r1, m1]')");

    Assert.assertNull(pigJob.getException());

    String query3 = "records1 = LOAD '"
        + table1path
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c, d, e, f, r1, m1', 'sorted');";

    System.out.println("query3:" + query3);
    pigServer.registerQuery(query3);   
    
    String foreach = "records11 = foreach records1 generate a as a, b as b, c as c, d as d, e as e, f as f, r1 as r1, m1 as m1;";
    pigServer.registerQuery(foreach);

    String join = "joinRecords = GROUP records11 BY " +  sortkey1 
        + " USING \"collected\";";
      pigServer.registerQuery(join);
   
    // check JOIN content
    Iterator<Tuple> it3 = pigServer.openIterator("joinRecords");
    return it3;
  }
  
  @Test
  public void test3() throws ExecException, IOException {
    Iterator<Tuple> it3 = groupby(pathTable1.toString(), "e" );
    verify(it3);
  }  
}
