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
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Note:
 * 
 * Make sure you add the build/pig-0.1.0-dev-core.jar to the Classpath of the
 * app/debug configuration, when run this from inside the Eclipse.
 * 
 */
public class TestBasicUnion extends BaseTestCase {

  final static String STR_SCHEMA1 = "a:string,b,c:string,e,f";
  final static String STR_STORAGE1 = "[a];[c]";
  final static String STR_SCHEMA2 = "a:string,b,d:string,f,e";
  final static String STR_STORAGE2 = "[a,b];[d]";
  final static String STR_SCHEMA3 = "b,a";
  final static String STR_STORAGE3 = "[a];[b]";
  final static String STR_SCHEMA4 = "b:string,a,c:string";
  final static String STR_STORAGE4 = "[a,b];[c]";
  final static String STR_SCHEMA5 = "b,a:string";
  final static String STR_STORAGE5 = "[a,b]";

  private static Path path1, path2, path3, path4, path5;

  @BeforeClass
  public static void setUp() throws Exception {

    init();

    path1 = getTableFullPath("/TestBasicUnion1");
    path2 = getTableFullPath("/TestBasicUnion2");
    path3 = getTableFullPath("/TestBasicUnion3");
    path4 = getTableFullPath("/TestBasicUnion4");
    path5 = getTableFullPath("/TestBasicUnion5");
    removeDir(path1);
    removeDir(path2);
    removeDir(path3);
    removeDir(path4);
    removeDir(path5);

    /*
     * create 1st basic table;
     */

    BasicTable.Writer writer = new BasicTable.Writer(path1, STR_SCHEMA1,
        STR_STORAGE1, conf);
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    final int numsBatch = 10;
    final int numsInserters = 2;
    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            if (k == 1 || k == 3 || k == 4) { // bytes type
              tuple.set(k, new DataByteArray(new String(b + "_" + i + "" + k).toString()));
            } else {
              tuple.set(k, b + "_" + i + "" + k);
            }
          } catch (ExecException e) {

          }
        }// k
        inserters[i].insert(new BytesWritable(("key1" + i).getBytes()), tuple);
      }// i
    }// b
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
    
    /*
     * create 2nd basic table;
     */
    writer = new BasicTable.Writer(path2, STR_SCHEMA2, STR_STORAGE2, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            if (k == 1 || k == 3 || k == 4) {
              tuple.set(k, new DataByteArray(new String(b + "_" + i + "" + k).toString()));
            } else {
              tuple.set(k, b + "_" + i + "" + k);
            }
          } catch (ExecException e) {

          }
        }
        inserters[i].insert(new BytesWritable(("key2" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
    
    /*
     * create 3rd basic table;
     */
    
    writer = new BasicTable.Writer(path3, STR_SCHEMA3, STR_STORAGE3, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, new DataByteArray(new String(b + "_" + i + "" + k).toString()));
          } catch (ExecException e) {

          }
        }
        inserters[i].insert(new BytesWritable(("key3" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
	  writer.close();
	
    /*
     * create 4th basic table;
     */

    writer = new BasicTable.Writer(path4, STR_SCHEMA4, STR_STORAGE4, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            if (k == 1) {
              tuple.set(k, new DataByteArray(new String(b + "_" + i + "" + k).toString()));
            } else {
              tuple.set(k, b + "_" + i + "" + k);
            }
          } catch (ExecException e) {

          }
        }
        inserters[i].insert(new BytesWritable(("key4" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
	  writer.close();
	  
    /*
     * create 5th basic table;
     */

    writer = new BasicTable.Writer(path5, STR_SCHEMA5, STR_STORAGE5, conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            if (k == 0) {
              tuple.set(k, new DataByteArray(new String(b + "_" + i + "" + k).toString()));
            } else {
              tuple.set(k, b + "_" + i + "" + k);
            }
          } catch (ExecException e) {

          }
        }
        inserters[i].insert(new BytesWritable(("key5" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
	  writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    pigServer.shutdown();
    BasicTable.drop(path1, conf);
    BasicTable.drop(path2, conf);
    BasicTable.drop(path3, conf);
    BasicTable.drop(path4, conf);
    BasicTable.drop(path5, conf);
  }

  @Test
  public void testReader1() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'a, b, c, d'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_00", cur.get(0));
        Assert.assertEquals(i + "_01", cur.get(1).toString());
        Assert.assertTrue(((cur.get(2) == null) || (cur.get(2)
            .equals(i + "_02"))));
        Assert.assertTrue(((cur.get(3) == null) || (cur.get(3)
            .equals(i + "_02"))));
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_10", cur.get(0));
        Assert.assertEquals(k + "_11", cur.get(1).toString());
        Assert.assertTrue(((cur.get(2) == null) || (cur.get(2)
            .equals(k + "_12"))));
        Assert.assertTrue(((cur.get(3) == null) || (cur.get(3)
            .equals(k + "_12"))));

      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_00", cur.get(0));
        Assert.assertEquals(t + "_01", cur.get(1).toString());
        Assert.assertTrue(((cur.get(2) == null) || (cur.get(2)
            .equals(t + "_02"))));
        Assert.assertTrue(((cur.get(3) == null) || (cur.get(3)
            .equals(t + "_02"))));
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_10", cur.get(0));
        Assert.assertEquals(j + "_11", cur.get(1).toString());
        Assert.assertTrue(((cur.get(2) == null) || (cur.get(2)
            .equals(j + "_12"))));
        Assert.assertTrue(((cur.get(3) == null) || (cur.get(3)
            .equals(j + "_12"))));
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  @Test
  public void testReaderThroughIO() throws ExecException, IOException,
    ParseException {

    String projection1 = new String("a,b,c");
    BasicTable.Reader reader = new BasicTable.Reader(path1, conf);
    reader.setProjection(projection1);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);

    System.out.println("read record or record:" + RowValue.toString());

    for (int i = 0; i <= 9; i++) {
      scanner.getValue(RowValue);
      System.out.println("read record or record:" + RowValue.toString());
      Assert.assertEquals(i + "_00", RowValue.get(0));
      Assert.assertEquals(i + "_01", RowValue.get(1).toString());
      Assert.assertEquals(i + "_02", RowValue.get(2));
      scanner.advance();
    }
    for (int i = 0; i <= 9; i++) {
      scanner.getValue(RowValue);
      System.out.println("read record or record:" + RowValue.toString());
      Assert.assertEquals(i + "_10", RowValue.get(0));
      Assert.assertEquals(i + "_11", RowValue.get(1).toString());
      Assert.assertEquals(i + "_12", RowValue.get(2));
      scanner.advance();
    }

    reader.close();
  }

  // field c
  @Test
  public void testReader2() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'c'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());

        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(i + "_02"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(k + "_12"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(t + "_02"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(j + "_12"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // projection for common exist colum a
  @Test
  public void testReader3() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'a'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(i + "_00"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(k + "_10"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(t + "_00"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0) == null) || (cur.get(0)
            .equals(j + "_10"))));

        try {
          cur.get(1);
          Assert.fail("should throw index out of bound exception ");
        } catch (Exception e) {

        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // some common fields
  @Test
  public void testReader4() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'a, b'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertEquals(i + "_00", cur.get(0));
        Assert.assertEquals(i + "_01", cur.get(1).toString());
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }

      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertEquals(k + "_10", cur.get(0));
        Assert.assertEquals(k + "_11", cur.get(1).toString());
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }
      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(t + "_00", cur.get(0));
        Assert.assertEquals(t + "_01", cur.get(1).toString());
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }
      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertEquals(j + "_10", cur.get(0));
        Assert.assertEquals(j + "_11", cur.get(1).toString());
        try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }
      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  // common column, but different posion
  @Test
  public void testReader5() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'e,f'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int k = -1;
    Tuple cur = null;
    int t = -1;
    int j = -1;

    while (it.hasNext()) {
      cur = it.next();

      System.out.println("cur: " + cur);
      // first table
      if (i <= 9) {
        System.out.println("first table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0)).toString().equals(i + "_03") || (cur.get(0).toString()
            .equals(i + "_04"))));
        System.out.println("get1: " + cur.get(1));
        Assert.assertTrue(((cur.get(1)).toString().equals(i + "_03") || (cur.get(1).toString()
            .equals(i + "_04"))));
        try {
          cur.get(2);
          Assert.fail("should throw out of index bound exception");
        } catch (Exception e) {

        }
      }
      if (i >= 10) {
        k++;
      }
      if (k <= 9 && k >= 0) {
        System.out.println("first table second part:  : " + cur.toString());
        Assert.assertTrue(((cur.get(0).toString().equals(k + "_13")) || (cur.get(0).toString()
            .equals(k + "_14"))));
        Assert.assertTrue(((cur.get(1).toString().equals(k + "_13")) || (cur.get(1).toString()
            .equals(k + "_14"))));

      }

      // second table
      if (k >= 10) {
        t++;
      }
      if (t <= 9 && t >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0).toString().equals(t + "_03")) || (cur.get(0).toString()
            .equals(t + "_04"))));
        Assert.assertTrue(((cur.get(1).toString().equals(t + "_03")) || (cur.get(1).toString()
            .equals(t + "_04"))));

      }
      if (t >= 10) {
        j++;
      }
      if (j <= 9 && j >= 0) {
        System.out.println("second table first part: " + cur.toString());
        Assert.assertTrue(((cur.get(0).toString().equals(j + "_13")) || (cur.get(0).toString()
            .equals(j + "_14"))));
        Assert.assertTrue(((cur.get(1).toString().equals(j + "_13")) || (cur.get(1).toString()
            .equals(j + "_14"))));

      }
      i++;
    }// while
    Assert.assertEquals(40, i);
  }

  @Test
  // union two tables with different column numbers and column positions
  public void testReader6() throws ExecException, IOException {
    pigServer.registerQuery(constructQuery(path1, path5, "'b,a'"));
    Iterator<Tuple> it = pigServer.openIterator("records");

    int i = 0;
    int count = 0;
    Tuple cur = null;

    String[] exp1 = new String[] { "0_01", "1_01", "2_01", "3_01", "4_01", "5_01", "6_01", "7_01", "8_01", "9_01", 
    		                       "0_11", "1_11", "2_11", "3_11", "4_11", "5_11", "6_11", "7_11", "8_11", "9_11"};
    String[] exp2 = new String[] { "0_00", "1_00", "2_00", "3_00", "4_00", "5_00", "6_00", "7_00", "8_00", "9_00", 
                                   "0_10", "1_10", "2_10", "3_10", "4_10", "5_10", "6_10", "7_10", "8_10", "9_10"};
    while (it.hasNext()) {
      count++;
      cur = it.next();
      System.out.println("cur #" + i + ": " + cur);
      try {
          cur.get(2);
          Assert.fail("should throw index out of bound exception");
      } catch (Exception e) {
      }

      //String b = (String)cur.get(0);
      String b = cur.get(0).toString();
      String a = (String)cur.get(1);
      Assert.assertTrue( b.equals(exp1[i]) || b.equals(exp2[i]) );
      Assert.assertTrue( a.equals(exp2[i]) || a.equals(exp1[i]) );
      if( ++i == 20 )
    	  i = 0;
    }// while
    Assert.assertEquals(40, count);
  }

  // both paths is hdfs:///../jars. mini cluster need to substr, real cluster
  // don't need to
  @Test
  public void testNeg1() throws ExecException, IOException {

      pigServer.registerQuery(constructQuery(path1, path2, "'a,b,c,d'"));
      Iterator<Tuple> it = pigServer.openIterator("records");

      int cnt = 0;
      Tuple cur = it.next();
      cnt++;
      while (it.hasNext()) {
          cur = it.next();
          System.out.println(cur);
          cnt++;
          if (cnt == 1) {
              Assert.assertEquals("0_00", cur.get(0));
              Assert.assertEquals("0_01", cur.get(1));
              Assert
                      .assertTrue(((cur.get(2) == null) || (cur.get(2).equals("0_02"))));
              Assert
                      .assertTrue(((cur.get(3) == null) || (cur.get(3).equals("0_02"))));
          }
          if (cnt == 22) {
              Assert.assertEquals("1_00", cur.get(0));
              Assert.assertEquals("1_01", cur.get(1).toString());
              Assert
                      .assertTrue(((cur.get(2) == null) || (cur.get(2).equals("1_02"))));
              Assert
                      .assertTrue(((cur.get(3) == null) || (cur.get(3).equals("1_02"))));

          }
      }
      Assert.assertEquals(cnt, 40);

  }

  // non-existing column
  @Test
  public void testNeg2() throws ExecException, IOException {

    pigServer.registerQuery(constructQuery(path1, path2, "'a,f,x'"));

    Iterator<Tuple> it = pigServer.openIterator("records");

    int cnt = 0;
    Tuple cur = it.next();
  //  cnt++;
    while (it.hasNext()) {
      cur = it.next();
      System.out.println(cur);
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("1_00", cur.get(0));
        System.out.println("neg2, cnt ==1: " +cur.get(1));
        Assert
            .assertTrue(((cur.get(1) == null) || (cur.get(1).toString().equals("1_03"))||(cur.get(1).toString().equals("1_04"))));
        Assert.assertEquals(null, cur.get(2));

        try {
          cur.get(3);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }
      }
      if (cnt == 21) {
        Assert.assertEquals("1_00", cur.get(0));
        System.out.println("neg2, cnt ==22: " +cur.get(1));
        
        Assert
            .assertTrue(((cur.get(1) == null) || (cur.get(1).toString().equals("1_04"))||(cur.get(1).toString().equals("1_03"))));
        Assert.assertEquals(null, cur.get(2));
        try {
          cur.get(3);
          Assert.fail("should throw index out of bound exception");
        } catch (Exception e) {

        }
      }
    }
    Assert.assertEquals(39, cnt);
  }

  @Test
  // 2 table with same column name but different type and different position.
  // should throw exception
  public void testNeg3() throws ExecException, IOException {
    try {
      pigServer.setValidateEachStatement(true);
      pigServer.registerQuery(constructQuery(path1, path3, "'a, b'"));
      Assert.fail("should throw exception");
    } catch (Exception e) {
    }
  }

    @Test
  // union table1 and table4. they have same culumn name with differnt types ,
  // should throw excepiton in union
  public void testNeg4() throws ExecException, IOException {
      try {
      pigServer.setValidateEachStatement(true);
      pigServer.registerQuery(constructQuery(path1, path4, "'a, b, c'"));
      Assert.fail("should throw exception");
    } catch (Exception e) {
    }
  }

  protected String constructQuery(Path path1, Path path2, String schema)
  {
    return "records = LOAD '" + path1 + "," + path2
            + "' USING org.apache.hadoop.zebra.pig.TableLoader(" + schema + ");";
  }
}
