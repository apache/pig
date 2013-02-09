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
import java.util.TreeSet;
import junit.framework.Assert;

import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.junit.Before;
import org.junit.Test;

public class TestStorageMap {
  String strSch = "m1:map(map(float)), m2:map(bool), f3:int";
  TableSchemaParser parser;
  Schema schema;

  @Before
  public void init() throws ParseException {
    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);

  }

  @Test
  public void testStorageValid1() {
    try {
      String strStorage = "[m1#{k1}]; [m2#{k1}, f3]";
      Partition p = new Partition(schema.toString(), strStorage, null);
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
      Assert.assertEquals(f11.getName(), "m1");
      Assert.assertEquals(ColumnType.MAP, f11.getType());

      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals(f21.getName(), "m2");
      // TODO: type should be MAP!
      Assert.assertEquals(ColumnType.MAP, f21.getType());

      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals(f22.getName(), "f3");
      Assert.assertEquals(ColumnType.INT, f22.getType());
      ColumnSchema f31 = cgs3.getSchema().getColumn(0);
      Assert.assertEquals(f31.getName(), "m1");
      Assert.assertEquals(ColumnType.MAP, f31.getType());
      ColumnSchema f32 = cgs3.getSchema().getColumn(1);
      Assert.assertEquals(f32.getName(), "m2");
      Assert.assertEquals(ColumnType.MAP, f32.getType());

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 3);
      Iterator<Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>> it = colmap
          .entrySet().iterator();
      for (int i = 0; i < colmap.size(); i++) {
        Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> entry = (Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>) it
            .next();
        String name = entry.getKey();
        HashSet<Partition.PartitionInfo.ColumnMappingEntry> hs = entry
            .getValue();
        TreeSet<Partition.PartitionInfo.ColumnMappingEntry> ts = new TreeSet<Partition.PartitionInfo.ColumnMappingEntry>(
            hs);
        Iterator<Partition.PartitionInfo.ColumnMappingEntry> it1 = ts
            .iterator();
        for (int j = 0; j < ts.size(); j++) {
          Partition.PartitionInfo.ColumnMappingEntry cme = (Partition.PartitionInfo.ColumnMappingEntry) it1
              .next();
          System.out.println("[Column = " + name + " CG = " + cme.getCGIndex()
              + "." + cme.getFieldIndex() + "]");
          if (i == 0 && j == 0) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 0 && j == 1) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "m2");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1 && j == 1) {
            Assert.assertEquals(name, "m2");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          } else if (i == 2 && j == 0) {
            Assert.assertEquals(name, "f3");
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
      String strStorage = "[m1#{k1}]; [m1#{k2}, f3]";
      Partition p = new Partition(schema.toString(), strStorage, null);
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
      Assert.assertEquals(f11.getName(), "m1");
      Assert.assertEquals(ColumnType.MAP, f11.getType());
      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals(f21.getName(), "m1");
      Assert.assertEquals(ColumnType.MAP, f21.getType());
      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals(f22.getName(), "f3");
      Assert.assertEquals(ColumnType.INT, f22.getType());
      ColumnSchema f31 = cgs3.getSchema().getColumn(0);
      Assert.assertEquals(f31.getName(), "m1");
      Assert.assertEquals(ColumnType.MAP, f31.getType());
      ColumnSchema f32 = cgs3.getSchema().getColumn(1);
      Assert.assertEquals(f32.getName(), "m2");
      Assert.assertEquals(ColumnType.MAP, f32.getType());

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 3);
      Iterator<Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>> it = colmap
          .entrySet().iterator();
      for (int i = 0; i < colmap.size(); i++) {
        Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> entry = (Map.Entry<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>>) it
            .next();
        String name = entry.getKey();
        HashSet<Partition.PartitionInfo.ColumnMappingEntry> hs = entry
            .getValue();
        TreeSet<Partition.PartitionInfo.ColumnMappingEntry> ts = new TreeSet<Partition.PartitionInfo.ColumnMappingEntry>(
            hs);
        Iterator<Partition.PartitionInfo.ColumnMappingEntry> it1 = ts
            .iterator();
        for (int j = 0; j < ts.size(); j++) {
          Partition.PartitionInfo.ColumnMappingEntry cme = (Partition.PartitionInfo.ColumnMappingEntry) it1
              .next();
          System.out.println("[Column = " + name + " CG = " + cme.getCGIndex()
              + "." + cme.getFieldIndex() + "]");
          if (i == 0 && j == 0) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 0 && j == 1) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 0 && j == 1) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "m2");
            Assert.assertEquals(cme.getCGIndex(), 2);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          } else if (i == 2 && j == 0) {
            Assert.assertEquals(name, "f3");
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
  public void testStorageInvalid1() {
    try {
      String strStorage = "m1#{k1}";
      Partition p = new Partition(schema.toString(), strStorage, null);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"m1 \"\" at line 1, column 1.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testStorageInvalid2() {
    try {
      String strStorage = "[m1#{k1}] abc; [m1#{k2}, f3] xyz";
      Partition p = new Partition(schema.toString(), strStorage, null);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" <IDENTIFIER> \"abc \"\" at line 1, column 11.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }

  @Test
  public void testStorageInvalid3() {
    try {
      String strStorage = "[m1{#k1}{#k2}]";
      Partition p = new Partition(schema.toString(), strStorage, null);
      CGSchema[] cgschemas = p.getCGSchemas();
      CGSchema cgs1 = cgschemas[0];
      System.out.println(cgs1);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \"{\" \"{ \"\" at line 1, column 4.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
}
