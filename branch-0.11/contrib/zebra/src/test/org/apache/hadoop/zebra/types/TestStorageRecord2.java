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

public class TestStorageRecord2 {
  String strSch = "r1:record(f1:int, f2:int), r2:record(f5:int, r3:record(f3:float, f4))";
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
      String strStorage = "[r1.f1, r2.r3.f3, r2.f5]; [r1.f2, r2.r3.f4]";
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
      Assert.assertEquals("r1.f1", f11.getName());
      Assert.assertEquals(ColumnType.INT, f11.getType());
      ColumnSchema f12 = cgs1.getSchema().getColumn(1);
      Assert.assertEquals("r2.r3.f3", f12.getName());
      Assert.assertEquals(ColumnType.FLOAT, f12.getType());

      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("r1.f2", f21.getName());
      Assert.assertEquals(ColumnType.INT, f21.getType());
      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals("r2.r3.f4", f22.getName());
      Assert.assertEquals(ColumnType.BYTES, f22.getType());

      System.out.println("*********** Column Map **********");
      Map<String, HashSet<Partition.PartitionInfo.ColumnMappingEntry>> colmap = p
          .getPartitionInfo().getColMap();
      Assert.assertEquals(colmap.size(), 5);
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
            Assert.assertEquals(name, "r2.f5");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 2);
          } else if (i == 1 && j == 0) {
            Assert.assertEquals(name, "r1.f1");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 2 && j == 0) {
            Assert.assertEquals(name, "r1.f2");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 0);
          } else if (i == 3 && j == 0) {
            Assert.assertEquals(name, "r2.r3.f3");
            Assert.assertEquals(cme.getCGIndex(), 0);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          } else if (i == 4 && j == 0) {
            Assert.assertEquals(name, "r2.r3.f4");
            Assert.assertEquals(cme.getCGIndex(), 1);
            Assert.assertEquals(cme.getFieldIndex(), 1);
          }
        }
      }
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }

  /*
   * @Test public void testStorageInvalid1() { try { String strStorage =
   * "m1#k1"; TableStorageParser parser = new TableStorageParser(new
   * ByteArrayInputStream(strStorage.getBytes("UTF-8")), null, schema);
   * ArrayList<CGSchema> schemas = parser.StorageSchema(); CGSchema cgs1 =
   * schemas.get(0); } catch (Exception e) { String errMsg = e.getMessage();
   * String str = "Encountered \" <IDENTIFIER> \"m1 \"\" at line 1, column 1.";
   * System.out.println(errMsg); System.out.println(str);
   * Assert.assertEquals(errMsg.startsWith(str), true); } }
   */
}
