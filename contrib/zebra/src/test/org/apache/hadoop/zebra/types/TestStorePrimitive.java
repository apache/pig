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
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import junit.framework.Assert;

import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.types.ColumnType;
import org.apache.hadoop.zebra.types.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Schema;
import org.apache.hadoop.zebra.types.TableSchemaParser;
import org.apache.hadoop.zebra.types.Schema.ColumnSchema;
import org.junit.Before;
import org.junit.Test;

public class TestStorePrimitive {
  String strSch = "f1:int, f2:long, f3:float, f4:bool, f5:string, f6:bytes";
  TableSchemaParser parser;
  Schema schema;

  @Before
  public void init() throws ParseException {
    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
  }

  @Test
  public void testSchema() throws ParseException {
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("f1", f1.name);
    Assert.assertEquals(ColumnType.INT, f1.type);
    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("f2", f2.name);
    Assert.assertEquals(ColumnType.LONG, f2.type);
    ColumnSchema f3 = schema.getColumn(2);
    Assert.assertEquals("f3", f3.name);
    Assert.assertEquals(ColumnType.FLOAT, f3.type);
    ColumnSchema f4 = schema.getColumn(3);
    Assert.assertEquals("f4", f4.name);
    Assert.assertEquals(ColumnType.BOOL, f4.type);
    ColumnSchema f5 = schema.getColumn(4);
    Assert.assertEquals("f5", f5.name);
    Assert.assertEquals(ColumnType.STRING, f5.type);
    ColumnSchema f6 = schema.getColumn(5);
    Assert.assertEquals("f6", f6.name);
    Assert.assertEquals(ColumnType.BYTES, f6.type);

    System.out.println(schema.toString());
  }

  @Test
  public void testStorageValid1() {
    try {
      String strStorage = "[f1, f2]; [f3, f4] COMPRESS BY gzip SERIALIZE BY avro";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();

      // 3 column group;
      int size = cgschemas.length;
      Assert.assertEquals(size, 3);
      System.out.println("********** Column Groups **********");
      for (int i = 0; i < cgschemas.length; i++) {
        System.out.println(cgschemas[i]);
        System.out.println("--------------------------------");
      }
      CGSchema cgs1 = cgschemas[0];
      CGSchema cgs2 = cgschemas[1];
      CGSchema cgs3 = cgschemas[2];

      ColumnSchema f11 = cgs1.getSchema().getColumn(0);
      Assert.assertEquals("f1", f11.name);
      Assert.assertEquals(ColumnType.INT, f11.type);
      ColumnSchema f12 = cgs1.getSchema().getColumn(1);
      Assert.assertEquals("f2", f12.name);
      Assert.assertEquals(ColumnType.LONG, f12.type);
      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("f3", f21.name);
      Assert.assertEquals(ColumnType.FLOAT, f21.type);
      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals("f4", f22.name);
      Assert.assertEquals(ColumnType.BOOL, f22.type);
      ColumnSchema f31 = cgs3.getSchema().getColumn(0);
      Assert.assertEquals("f5", f31.name);
      Assert.assertEquals(ColumnType.STRING, f31.type);
      ColumnSchema f32 = cgs3.getSchema().getColumn(1);
      Assert.assertEquals("f6", f32.name);
      Assert.assertEquals(ColumnType.BYTES, f32.type);

      Assert.assertEquals(cgs1.getCompressor(), "lzo2");
      Assert.assertEquals(cgs1.getSerializer(), "pig");
      Assert.assertEquals(cgs2.getCompressor(), "gzip");
      Assert.assertEquals(cgs2.getSerializer(), "avro");
      Assert.assertEquals(cgs3.getCompressor(), "lzo2");
      Assert.assertEquals(cgs3.getSerializer(), "pig");

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 6);
      Iterator<Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>> it = colmap
          .entrySet().iterator();
      for (int i = 0; i < colmap.size(); i++) {
        Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> entry = (Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>) it
            .next();
        String name = entry.getKey();
        HashSet<Partition.PartitionInfo.ColumnMappingEntry> hs = entry
            .getValue();
        Iterator<Partition.PartitionInfo.ColumnMappingEntry> it1 = hs
            .iterator();
        for (int j = 0; j < hs.size(); j++) {
          Partition.PartitionInfo.ColumnMappingEntry cme = (Partition.PartitionInfo.ColumnMappingEntry) it1
              .next();
          System.out.println("[Column = " + name + " CG = " + cme.getCGIndex()
              + "." + cme.getFieldIndex() + "]");
          if (i == 0 && j == 0) {
            Assert.assertEquals(name, "f6");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "f1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 2 && j == 0) {
            Assert.assertEquals(name, "f3");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 3 && j == 0) {
            Assert.assertEquals(name, "f2");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          } else if (i == 4 && j == 0) {
            Assert.assertEquals(name, "f5");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 5 && j == 0) {
            Assert.assertEquals(name, "f4");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          }
        }
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testStorageValid2() {
    try {
      String strStorage = "[f1, f2] serialize by avro compress by gzip; [f3, f4] SERIALIZE BY avro COMPRESS BY gzip";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();

      Assert.assertEquals(cgschemas.length, 3);
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testStorageValid3() {
    try {
      String strStorage = "";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      Assert.assertEquals(cgschemas.length, 1);
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testStorageInvalid1() {
    try {
      String strStorage = "f1";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"f1 \"\" at line 1, column 1.\nWas expecting one of:\n    <EOF> \n    \"compress by\" ...\n    \"serialize by\" ...\n    \"[\" ...\n    ";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid2() {
    try {
      String strStorage = "[f100]";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Column f100 not defined in schema";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid3() {
    try {
      String strStorage = "f1:long";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"f1 \"\" at line 1, column 1.\nWas expecting one of:\n    <EOF> \n    \"compress by\" ...\n    \"serialize by\" ...\n    \"[\" ...\n    ";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid4() {
    try {
      String strStorage = "[";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \"[\" \"[ \"\" at line 1, column 1.\nWas expecting one of:\n    \"compress by\" ...\n    \"serialize by\" ...\n    ";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid5() {
    try {
      String strStorage = "[f1, f2]; [f1, f4]";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Column f1 specified more than once!";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid6() {
    try {
      String strStorage = ":";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Lexical error at line 1, column 2.  Encountered: <EOF> after : \"\"";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg, str);
    }
  }

  @Test
  public void testStorageInvalid7() {
    try {
      String strStorage = "[f1, f2] serialize by xyz compress by gzip; [f3, f4] SERIALIZE BY avro COMPRESS BY lzo2";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \"serialize by\" \"serialize by \"\" at line 1, column 10.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testStorageInvalid8() {
    try {
      String strStorage = "[f1, f2] serialize by avro compress by xyz; [f3, f4] SERIALIZE BY avro COMPRESS BY lzo2";
      Partition p = new Partition(schema.toString(), strStorage);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \"compress by\" \"compress by \"\" at line 1, column 28.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
}