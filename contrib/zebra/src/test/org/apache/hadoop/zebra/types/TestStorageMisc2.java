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
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.junit.Before;
import org.junit.Test;

public class TestStorageMisc2 {
  String strSch = "c:collection(r:record(r:record(f1:int, f2:int), f2:map)), m1:map(int)";
  TableSchemaParser parser;
  Schema schema;

  @Before
  public void init() throws ParseException {
    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
  }

  @Test
  public void testSchema() throws ParseException {
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("c", f1.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f1.getType());
    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("m1", f2.getName());
    Assert.assertEquals(ColumnType.MAP, f2.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0);
    Assert.assertEquals("r", f11.getName());
    Assert.assertEquals(ColumnType.RECORD, f11.getType());

    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0);
    // Assert.assertEquals("", f21.getName());
    Assert.assertEquals(ColumnType.INT, f21.getType());

    // test 3rd level schema;
    Schema f11Schema = f11.getSchema();
    ColumnSchema f111 = f11Schema.getColumn(0);
    Assert.assertEquals("r", f111.getName());
    Assert.assertEquals(ColumnType.RECORD, f111.getType());
    ColumnSchema f112 = f11Schema.getColumn(1);
    Assert.assertEquals("f2", f112.getName());
    Assert.assertEquals(ColumnType.MAP, f112.getType());

    // test 4th level schema;
    Schema f111Schema = f111.getSchema();
    ColumnSchema f1111 = f111Schema.getColumn(0);
    Assert.assertEquals("f1", f1111.getName());
    Assert.assertEquals(ColumnType.INT, f1111.getType());
    ColumnSchema f1112 = f111Schema.getColumn(1);
    Assert.assertEquals("f2", f1112.getName());
    Assert.assertEquals(ColumnType.INT, f1112.getType());

    Schema f112Schema = f112.getSchema();
    ColumnSchema f1121 = f112Schema.getColumn(0);
    // Assert.assertEquals("", f1121.getName());
    Assert.assertEquals(ColumnType.BYTES, f1121.getType());
  }

  @Test
  public void testStorageValid1() {
    try {
      String strStorage = "[c] compress by gz; [m1] serialize by avro";
      Partition p = new Partition(schema.toString(), strStorage, null);
      CGSchema[] cgschemas = p.getCGSchemas();

      // 2 column group;
      int size = cgschemas.length;
      Assert.assertEquals(size, 2);
      System.out.println("********** Column Groups **********");
      for (int i = 0; i < cgschemas.length; i++) {
        System.out.println(cgschemas[i]);
        System.out.println("--------------------------------");
      }
      CGSchema cgs1 = cgschemas[0];
      CGSchema cgs2 = cgschemas[1];

      ColumnSchema f11 = cgs1.getSchema().getColumn(0);
      Assert.assertEquals("c", f11.getName());
      Assert.assertEquals(ColumnType.COLLECTION, f11.getType());

      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("m1", f21.getName());
      Assert.assertEquals(ColumnType.MAP, f21.getType());

      Assert.assertEquals(cgs1.getCompressor(), "gz");
      Assert.assertEquals(cgs2.getSerializer(), "avro");

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 2);
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
            Assert.assertEquals(name, "c");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "m1");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
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
      String strStorage = "[c] compress by gz; [m1#{k1}] serialize by avro";
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
      Assert.assertEquals("c", f11.getName());
      Assert.assertEquals(ColumnType.COLLECTION, f11.getType());

      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("m1", f21.getName());
      Assert.assertEquals(ColumnType.MAP, f21.getType());

      ColumnSchema f31 = cgs3.getSchema().getColumn(0);
      Assert.assertEquals("m1", f31.getName());
      Assert.assertEquals(ColumnType.MAP, f31.getType());

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 2);
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
          /*
           * if (i == 0 && j == 0) { Assert.assertEquals(name, "c");
           * Assert.assertEquals(cme.getCGIndex(), 0);
           * Assert.assertEquals(cme.getFieldIndex(), 0); } else if (i == 0 && j
           * == 1) { Assert.assertEquals(name, "m1");
           * Assert.assertEquals(cme.getCGIndex(), 1);
           * Assert.assertEquals(cme.getFieldIndex(), 0); } else if (i == 1 && j
           * == 0) { Assert.assertEquals(name, "m1");
           * Assert.assertEquals(cme.getCGIndex(), 2);
           * Assert.assertEquals(cme.getFieldIndex(), 0); }
           */
        }
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }
}