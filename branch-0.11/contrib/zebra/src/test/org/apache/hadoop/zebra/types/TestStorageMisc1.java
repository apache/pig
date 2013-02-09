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

public class TestStorageMisc1 {
  String strSch = "r:record(r:record(f1:int, f2:int), f2:map)";
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
    Assert.assertEquals("r", f1.getName());
    Assert.assertEquals(ColumnType.RECORD, f1.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0);
    Assert.assertEquals("r", f11.getName());
    Assert.assertEquals(ColumnType.RECORD, f11.getType());
    ColumnSchema f12 = f1Schema.getColumn(1);
    Assert.assertEquals("f2", f12.getName());
    Assert.assertEquals(ColumnType.MAP, f12.getType());

    // test 3rd level schema;
    Schema f11Schema = f11.getSchema();
    ColumnSchema f111 = f11Schema.getColumn(0);
    Assert.assertEquals("f1", f111.getName());
    Assert.assertEquals(ColumnType.INT, f111.getType());
    ColumnSchema f112 = f11Schema.getColumn(1);
    Assert.assertEquals("f2", f112.getName());
    Assert.assertEquals(ColumnType.INT, f112.getType());

    Schema f12Schema = f12.getSchema();
    ColumnSchema f121 = f12Schema.getColumn(0);
    // Assert.assertEquals("", f121.getName());
    Assert.assertEquals(ColumnType.BYTES, f121.getType());
  }

  @Test
  public void testStorageValid1() {
    try {
        String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gz SECURE BY uid:root; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY uid:root; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY uid:root group:data perm:0766; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY uid:root group:data perm:966; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY uid:ggg SECURE BY group:fff; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";
//      String strStorage = "[r.r.f1,r.f2#{k1}] COMPRESS BY gzip SECURE BY uid:root user:root; [r.r.f2, r.f2#{k2}] COMPRESS BY lzo SERIALIZE BY avro";

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
      Assert.assertEquals("r.r.f1", f11.getName());
      Assert.assertEquals(ColumnType.INT, f11.getType());
      ColumnSchema f12 = cgs1.getSchema().getColumn(1);
      Assert.assertEquals("r.f2", f12.getName());
      Assert.assertEquals(ColumnType.MAP, f12.getType());

      ColumnSchema f21 = cgs2.getSchema().getColumn(0);
      Assert.assertEquals("r.r.f2", f21.getName());
      Assert.assertEquals(ColumnType.INT, f21.getType());
      ColumnSchema f22 = cgs2.getSchema().getColumn(1);
      Assert.assertEquals("r.f2", f22.getName());
      Assert.assertEquals(ColumnType.MAP, f22.getType());

      ColumnSchema f31 = cgs3.getSchema().getColumn(0);
      Assert.assertEquals("r.f2", f31.getName());
      Assert.assertEquals(ColumnType.MAP, f31.getType());

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
          /*
           * if (i == 0 && j == 0) { Assert.assertEquals(name, "r.r.f1");
           * Assert.assertEquals(cme.getCGIndex(), 0);
           * Assert.assertEquals(cme.getFieldIndex(), 0); } else if (i == 1 && j
           * == 0) { Assert.assertEquals(name, "r.r.f2");
           * Assert.assertEquals(cme.getCGIndex(), 1);
           * Assert.assertEquals(cme.getFieldIndex(), 0); } else if (i == 2 && j
           * == 0) { Assert.assertEquals(name, "r.f2");
           * Assert.assertEquals(cme.getCGIndex(), 1);
           * Assert.assertEquals(cme.getFieldIndex(), 1); } else if (i == 2 && j
           * == 1) { Assert.assertEquals(name, "r.f2");
           * Assert.assertEquals(cme.getCGIndex(), 2);
           * Assert.assertEquals(cme.getFieldIndex(), 0); } else if (i == 2 && j
           * == 2) { Assert.assertEquals(name, "r.f2");
           * Assert.assertEquals(cme.getCGIndex(), 0);
           * Assert.assertEquals(cme.getFieldIndex(), 1); }
           */
        }
      }
    } catch (Exception e) {
      System.out.println("Error is [" + e.getMessage() + "]");
      Assert.assertTrue(false);
    }
  }
}
