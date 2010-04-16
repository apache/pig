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

public class TestSchemaMap {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "f1:int, m1:map(int)";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("f1", f1.getName());
    Assert.assertEquals(ColumnType.INT, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("m1", f2.getName());
    Assert.assertEquals(ColumnType.MAP, f2.getType());
    
    Schema temp = f2.getSchema();
    System.out.println(temp.getNumColumns());
    ColumnSchema cs = temp.getColumn(0);
    System.out.println(cs.getType());
    
  }

  @Test
  public void testSchemaValid2() throws ParseException {
    String strSch = "f1:int, m1:map(map(float))";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("f1", f1.getName());
    Assert.assertEquals(ColumnType.INT, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("m1", f2.getName());
    Assert.assertEquals(ColumnType.MAP, f2.getType());

    // test 2nd level schema;
    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0);
    // Assert.assertEquals("m1", f2.getName());
    Assert.assertEquals(ColumnType.MAP, f21.getType());

    // test 3rd level schema;
    Schema f21Schema = f21.getSchema();
    ColumnSchema f211 = f21Schema.getColumn(0);
    // Assert.assertEquals("m1", f2.getName());
    Assert.assertEquals(ColumnType.FLOAT, f211.getType());
  }

  @Test
  public void testSchemaValid3() throws ParseException {
    String strSch = "m1:map(map(float)), m2:map(bool), f3";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("m1", f1.getName());
    Assert.assertEquals(ColumnType.MAP, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("m2", f2.getName());
    Assert.assertEquals(ColumnType.MAP, f2.getType());

    ColumnSchema f3 = schema.getColumn(2);
    Assert.assertEquals("f3", f3.getName());
    Assert.assertEquals(ColumnType.BYTES, f3.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0);
    Assert.assertEquals(ColumnType.MAP, f11.getType());

    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0);
    Assert.assertEquals(ColumnType.BOOL, f21.getType());

    // test 3rd level schema;
    Schema f11Schema = f11.getSchema();
    ColumnSchema f111 = f11Schema.getColumn(0);
    Assert.assertEquals(ColumnType.FLOAT, f111.getType());
  }

  @Test
  public void testSchemaInvalid1() throws ParseException {
    try {
      String strSch = "m1:abc";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"abc \"\" at line 1, column 4.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid2() throws ParseException {
    try {
      String strSch = "m1:map(int";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \"<EOF>\" at line 1, column 10.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid3() throws ParseException {
    try {
      String strSch = "m1:map(int, f2:int";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \",\" \", \"\" at line 1, column 11.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid4() throws ParseException {
    try {
      String strSch = "m1:map(m2:int)";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"m2 \"\" at line 1, column 8.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid5() throws ParseException {
    try {
      String strSch = "m1:map(abc)";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"abc \"\" at line 1, column 8.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
}
