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
import java.util.StringTokenizer;

import junit.framework.Assert;
import junit.framework.TestCase;

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
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hadoop.zebra.BaseTestCase;


/**
 * Note:
 * 
 * Make sure you add the build/pig-0.1.0-dev-core.jar to the Classpath of the
 * app/debug configuration, when run this from inside the Eclipse.
 * 
 */
public class TestTableSortStorer extends BaseTestCase {
  private static Path pathTable;

  @BeforeClass
  public static void setUp() throws Exception {

    init();
    pathTable = getTableFullPath("TestTableSortStorer");
    removeDir(pathTable);

    BasicTable.Writer writer = new BasicTable.Writer(pathTable,
        "SF_a:string,SF_b:string,SF_c:string,SF_d:string,SF_e:string,SF_f:string,SF_g:string",
        "[SF_a, SF_b, SF_c]; [SF_e, SF_f, SF_g]", conf);
    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);

    final int numsBatch = 10;
    final int numsInserters = 1;
    TableInserter[] inserters = new TableInserter[numsInserters];
    for (int i = 0; i < numsInserters; i++) {
      inserters[i] = writer.getInserter("ins" + i, false);
    }

    for (int b = 0; b < numsBatch; b++) {
      for (int i = 0; i < numsInserters; i++) {
        TypesUtils.resetTuple(tuple);
        for (int k = 0; k < tuple.size(); ++k) {
          try {
            tuple.set(k, (9-b) + "_" + i + "" + k);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
        inserters[i].insert(new BytesWritable(("key" + i).getBytes()), tuple);
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

  @Test
  public void testStorer() throws ExecException, IOException {
    /*
     * Use pig LOAD to load testing data for store
     */
    String query = "records = LOAD '" + pathTable.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    pigServer.registerQuery(query);

    Iterator<Tuple> it2 = pigServer.openIterator("records");
    int row0 = 0;
    Tuple RowValue2 = null;
    while (it2.hasNext()) {
      // Last row value
      RowValue2 = it2.next();
      row0++;
      if (row0 == 10) {
        Assert.assertEquals("0_01", RowValue2.get(1));
        Assert.assertEquals("0_00", RowValue2.get(0).toString());
      }
    }
    Assert.assertEquals(10, row0);

    String orderby = "srecs = ORDER records BY SF_a;";
    pigServer.registerQuery(orderby);

    /*
     * Use pig STORE to store testing data BasicTable.Writer writer = new
     * BasicTable.Writer(pathTable, "SF_a,SF_b,SF_c,SF_d,SF_e,SF_f,SF_g",
     * "[SF_a, SF_b, SF_c]; [SF_e, SF_f, SF_g]", false, conf);
     */
    Path newPath = getTableFullPath(getCurrentMethodName());
    removeDir(newPath);
    pigServer
        .store(
            "srecs",
            newPath.toString(),
            TableStorer.class.getCanonicalName()
                + "('[SF_a, SF_b, SF_c]; [SF_e]')");

    // check new table content
    String query3 = "newRecords = LOAD '"
        + newPath.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('SF_a, SF_b');";
    pigServer.registerQuery(query3);

    Iterator<Tuple> it3 = pigServer.openIterator("newRecords");
    int row = 0;
    Tuple RowValue3 = null;
    while (it3.hasNext()) {
      // Last row value
      RowValue3 = it3.next();
      row++;
      if (row == 10) {
        Assert.assertEquals("9_01", RowValue3.get(1));
        Assert.assertEquals("9_00", RowValue3.get(0).toString());
      }
    }
    Assert.assertEquals(10, row);
  }
}
