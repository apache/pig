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

package org.apache.hadoop.zebra.types;

import java.io.StringReader;
import junit.framework.Assert;

import org.apache.hadoop.zebra.types.ColumnType;
import org.apache.hadoop.zebra.types.ParseException;
import org.apache.hadoop.zebra.types.Schema;
import org.apache.hadoop.zebra.types.TableSchemaParser;
import org.apache.hadoop.zebra.types.Schema.ColumnSchema;
import org.junit.Test;

public class TestSchemaCollection {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "c1:collection(f1:int, f2:int), c2:collection(c3:collection(f3:float, f4))";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("c1", f1.name);
    Assert.assertEquals(ColumnType.COLLECTION, f1.type);

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("c2", f2.name);
    Assert.assertEquals(ColumnType.COLLECTION, f2.type);

    // test 2nd level schema;
    Schema f1Schema = f1.schema;
    ColumnSchema f11 = f1Schema.getColumn(0);
    Assert.assertEquals("f1", f11.name);
    Assert.assertEquals(ColumnType.INT, f11.type);
    ColumnSchema f12 = f1Schema.getColumn(1);
    Assert.assertEquals("f2", f12.name);
    Assert.assertEquals(ColumnType.INT, f12.type);

    Schema f2Schema = f2.schema;
    ColumnSchema f21 = f2Schema.getColumn(0);
    Assert.assertEquals("c3", f21.name);
    Assert.assertEquals(ColumnType.COLLECTION, f21.type);

    // test 3rd level schema;
    Schema f21Schema = f21.schema;
    ColumnSchema f211 = f21Schema.getColumn(0);
    Assert.assertEquals("f3", f211.name);
    Assert.assertEquals(ColumnType.FLOAT, f211.type);
    ColumnSchema f212 = f21Schema.getColumn(1);
    Assert.assertEquals("f4", f212.name);
    Assert.assertEquals(ColumnType.BYTES, f212.type);
  }

  @Test
  public void testSchemaInvalid1() throws ParseException, Exception {
    try {
      String strSch = "c1:collection(f1:int, f2:int), c2:collection(c3:collection(f3:float, f4)))";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \")\" \") \"\" at line 1, column 74.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
}