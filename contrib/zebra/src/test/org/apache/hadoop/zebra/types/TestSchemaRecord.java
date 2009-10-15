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

import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.junit.Test;

public class TestSchemaRecord {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "r1:record(f1:int, f2:int), r2:record(r3:record(f3:float, f4))";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("r1", f1.getName());
    Assert.assertEquals(ColumnType.RECORD, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("r2", f2.getName());
    Assert.assertEquals(ColumnType.RECORD, f2.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0);
    Assert.assertEquals("f1", f11.getName());
    Assert.assertEquals(ColumnType.INT, f11.getType());
    ColumnSchema f12 = f1Schema.getColumn(1);
    Assert.assertEquals("f2", f12.getName());
    Assert.assertEquals(ColumnType.INT, f12.getType());

    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0);
    Assert.assertEquals("r3", f21.getName());
    Assert.assertEquals(ColumnType.RECORD, f21.getType());

    // test 3rd level schema;
    Schema f21Schema = f21.getSchema();
    ColumnSchema f211 = f21Schema.getColumn(0);
    Assert.assertEquals("f3", f211.getName());
    Assert.assertEquals(ColumnType.FLOAT, f211.getType());
    ColumnSchema f212 = f21Schema.getColumn(1);
    Assert.assertEquals("f4", f212.getName());
    Assert.assertEquals(ColumnType.BYTES, f212.getType());
  }

  /*
   * @Test public void testSchemaInvalid1() throws ParseException { try { String
   * strSch = "m1:abc"; TableSchemaParser parser; Schema schema;
   * 
   * parser = new TableSchemaParser(new StringReader(strSch)); schema =
   * parser.RecordSchema(); System.out.println(schema); } catch (Exception e) {
   * String errMsg = e.getMessage(); String str =
   * "Encountered \" <IDENTIFIER> \"abc \"\" at line 1, column 4.";
   * System.out.println(errMsg); System.out.println(str);
   * Assert.assertEquals(errMsg.startsWith(str), true); } }
   */
}
