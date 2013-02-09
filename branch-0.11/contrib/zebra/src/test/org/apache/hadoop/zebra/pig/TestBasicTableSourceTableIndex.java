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
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestBasicTableSourceTableIndex extends BaseTestCase {
  private static Path pathTable1, pathTable2;
  
  @BeforeClass
  public static void setUp() throws Exception {
    init();
    
    pathTable1 = getTableFullPath("TestBasicTableSourceTableIndex1");
    pathTable2 = getTableFullPath("TestBasicTableSourceTableIndex2");    
    removeDir(pathTable1);
    removeDir(pathTable2);

    /*
     * create 1st basic table;
     */

    BasicTable.Writer writer = new BasicTable.Writer(pathTable1,
        "a:string,b:string,c:string", "[a,b];[c]", conf);
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
            tuple.set(k, b + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key1" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
    writer.close();
    
    /*
     * create 2nd basic table;
     */

    writer = new BasicTable.Writer(pathTable2, "a:string,b:string,d:string",
        "[a,b];[d]", conf);
    schema = writer.getSchema();
    tuple = TypesUtils.createTuple(schema);

    inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    int j;
    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        j = i + 2;
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, b + "_" + j + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key2" + i).getBytes()), tuple);
      }
    }
    for (int i = 0; i < numsInserters; i++) {
      inserters[i].close();
    }
	  writer.close();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    pigServer.shutdown();
  }

  @Test
  public void testReader() throws ExecException, IOException {
    String str1 = pathTable1.toString();
    String str2 = pathTable2.toString();    

    String query = "records = LOAD '" + str1 + "," + str2
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('source_table, a, b, c, d');";
    System.out.println(query);
    pigServer.registerQuery(query);
    
 // Verify union table
    HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable
        = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
    
    ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_00",  i+"_01",  i+"_02");
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_10",  i+"_11",  i+"_12");
    resultTable.put(0, rows);
    
    rows = new ArrayList<ArrayList<Object>>();
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_20",  i+"_21",  i+"_22");
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_30",  i+"_31",  i+"_32");
    resultTable.put(1, rows);
    
    // Verify union table
    Iterator<Tuple> it = pigServer.openIterator("records");
    int numbRows = verifyTable(resultTable, 1, 0, it);
    
    Assert.assertEquals(numbRows, 40);
  }
  
  /**
   * Test source table index from a single table
   * @throws ExecException
   * @throws IOException
   */
  @Test
  public void testSingleReader() throws ExecException, IOException {
    String str1 = pathTable1.toString();    

    String query = "records = LOAD '" + str1
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('source_table, a, b, c, d');";
    System.out.println(query);
    pigServer.registerQuery(query);
    
 // Verify union table
    HashMap<Integer, ArrayList<ArrayList<Object>>> resultTable
        = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
    
    ArrayList<ArrayList<Object>> rows = new ArrayList<ArrayList<Object>>();
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_00",  i+"_01",  i+"_02");
    for (int i = 0; i < 10; i++)
      addResultRow(rows, 0,  i+"_10",  i+"_11",  i+"_12");
    resultTable.put(0, rows);
    
    // Verify union table
    Iterator<Tuple> it = pigServer.openIterator("records");
    int numbRows = verifyTable(resultTable, 1, 0, it);
    
    Assert.assertEquals(numbRows, 20);
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
