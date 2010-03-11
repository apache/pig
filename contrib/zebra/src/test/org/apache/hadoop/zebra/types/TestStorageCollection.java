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
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
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

public class TestStorageCollection {
  String strSch = "c1:collection(record(f1:int, f2:int)), c2:collection(record(c3:collection(record(f3:float, f4))))";
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
      String strStorage = "[c1]; [c2]";
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
      Assert.assertEquals("c1", f11.getName());
      Assert.assertEquals(ColumnType.COLLECTION, f11.getType());
      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("c2", f21.getName());
      Assert.assertEquals(ColumnType.COLLECTION, f21.getType());

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
        while (it1.hasNext()) {
          Partition.PartitionInfo.ColumnMappingEntry cme = (Partition.PartitionInfo.ColumnMappingEntry) it1
              .next();
          System.out.println("[Column = " + name + " CG = " + cme.getCGIndex()
              + "." + cme.getFieldIndex() + "]");
          if (i == 0) {
            Assert.assertEquals(name, "c1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1) {
            Assert.assertEquals(name, "c2");
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
      String strStorage = "[c1.f1]";
      Partition p = new Partition(schema.toString(), strStorage, null);
      Assert.assertTrue(false);
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
      Assert.assertEquals("c1.f1", f11.getName());
      Assert.assertEquals(ColumnType.INT, f11.getType());
      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("c1.f2", f21.getName());
      Assert.assertEquals(ColumnType.INT, f21.getType());
      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals("c2", f22.getName());
      Assert.assertEquals(ColumnType.COLLECTION, f22.getType());

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
        Iterator<Partition.PartitionInfo.ColumnMappingEntry> it1 = hs
            .iterator();
        for (int j = 0; j < hs.size(); j++) {
          Partition.PartitionInfo.ColumnMappingEntry cme = (Partition.PartitionInfo.ColumnMappingEntry) it1
              .next();
          System.out.println("[Column = " + name + " CG = " + cme.getCGIndex()
              + "." + cme.getFieldIndex() + "]");
          if (i == 0 && j == 0) {
            Assert.assertEquals(name, "c1.f2");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "c1.f1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 2 && j == 0) {
            Assert.assertEquals(name, "c2");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          }
        }
      }
    } catch (Exception e) {
    }
  }
}
