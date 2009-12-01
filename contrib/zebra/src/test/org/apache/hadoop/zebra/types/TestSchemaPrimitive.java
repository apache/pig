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

public class TestSchemaPrimitive {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "f1:int, f2:long, f3:float, f4:bool, f5:string, f6:bytes";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("f2", f2.getName());
    Assert.assertEquals(ColumnType.LONG, f2.getType());
  }

  @Test
  public void testSchemaValid2() throws ParseException {
    String strSch = "f1:int, f2, f3:float, f4, f5:string, f6::f61, f7::f71:map";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("f2", f2.getName());
    Assert.assertEquals(ColumnType.BYTES, f2.getType());

    ColumnSchema f4 = schema.getColumn(3);
    Assert.assertEquals("f4", f4.getName());
    Assert.assertEquals(ColumnType.BYTES, f4.getType());
    
    ColumnSchema f6 = schema.getColumn(5);
    Assert.assertEquals("f6::f61", f6.getName());
    Assert.assertEquals(ColumnType.BYTES, f6.getType());
    
    ColumnSchema f7 = schema.getColumn(6);
    Assert.assertEquals("f7::f71", f7.getName());
    Assert.assertEquals(ColumnType.MAP, f7.getType());
  }

  /*
   * @Test public void testSchemaValid3() throws ParseException { try { String
   * strSch = ",f1:int"; TableSchemaParser parser; Schema schema;
   * 
   * parser = new TableSchemaParser(new StringReader(strSch)); schema =
   * parser.RecordSchema(null); System.out.println(schema);
   * 
   * ColumnSchema f1 = schema.getColumn(0); //Assert.assertEquals("f2",
   * f2.getName()); //Assert.assertEquals(ColumnType.BYTES, f2.getType());
   * 
   * ColumnSchema f2 = schema.getColumn(1); //Assert.assertEquals("f1",
   * f4.getName()); //Assert.assertEquals(ColumnType.BYTES, f4.getType()); } catch
   * (Exception e) { System.out.println(e.getMessage()); } }
   */

  @Test
  public void testSchemaInvalid1() {
    try {
      String strSch = "f1:int, f2:xyz, f3:float";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"xyz \"\" at line 1, column 12.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid2() {
    try {
      String strSch = "f1:";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \"<EOF>\" at line 1, column 3.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid3() {
    try {
      String strSch = ":";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \":\" \": \"\" at line 1, column 1.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testSchemaInvalid4() {
    try {
      String strSch = "f1:int abc";
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
  
  @Test
  public void testSchemaInvalid5() {
    try {
      String strSch = "f1:f11";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"f11 \"\" at line 1, column 4.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
}
