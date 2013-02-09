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

public class TestSchemaAnonymousCollection {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "c1:collection(Record(f1:int, f2:int)), c2:collection(record(f5:collection(record(f3:float, f4))))";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("c1", f1.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("c2", f2.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f2.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0).getSchema().getColumn(0);
    Assert.assertEquals("f1", f11.getName());
    Assert.assertEquals(ColumnType.INT, f11.getType());
    ColumnSchema f12 = f1Schema.getColumn(0).getSchema().getColumn(1);
    Assert.assertEquals("f2", f12.getName());
    Assert.assertEquals(ColumnType.INT, f12.getType());

    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0);
    Assert.assertNull(f21.getName());
    Assert.assertEquals(ColumnType.RECORD, f21.getType());

    // test 3rd level schema;
    Schema f21Schema = f21.getSchema();
    ColumnSchema f211 = f21Schema.getColumn(0);
    Assert.assertTrue(f211.getName().equals("f5"));
    Assert.assertEquals(ColumnType.COLLECTION, f211.getType());
    Schema f211Schema = f211.getSchema();
    
    ColumnSchema f212 = f211Schema.getColumn(0);
    Assert.assertNull(f212.getName());
    Assert.assertEquals(ColumnType.RECORD, f212.getType());
    Schema f212Schema = f212.getSchema();
    ColumnSchema f213 = f212Schema.getColumn(0);
    Assert.assertEquals("f3", f213.getName());
    Assert.assertEquals(ColumnType.FLOAT, f213.getType());
    ColumnSchema f214 = f212Schema.getColumn(1);
    Assert.assertEquals("f4", f214.getName());
    Assert.assertEquals(ColumnType.BYTES, f214.getType());
  }
}