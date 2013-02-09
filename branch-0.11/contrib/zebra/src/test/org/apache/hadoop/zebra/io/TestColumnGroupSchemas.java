/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.io;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnGroupSchemas {

  @BeforeClass
  public static void setUpOnce() throws IOException {
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  @Test
  public void testBlankSchema() throws IOException, ParseException {
    System.out.println("testBlankSchema");
    Schema schema = Schema.parse(" ");
    Assert.assertEquals(0, schema.getColumns().length);
  }

  @Test
  public void test1Column() throws IOException, ParseException {
    System.out.println("test1SimpleColumn");
    Schema schema = Schema.parse("  abc");
    Assert.assertEquals(1, schema.getColumns().length);
    Assert.assertEquals("abc", schema.getColumns()[0]);
  }

  @Test
  public void test2Columns() throws IOException, ParseException {
    System.out.println("test2SimpleColumns");
    Schema schema = Schema.parse("abc, def ");
    Assert.assertEquals(2, schema.getColumns().length);
    Assert.assertEquals("abc", schema.getColumns()[0]);
    Assert.assertEquals("def", schema.getColumns()[1]);
  }

  @Test
  public void test3Columns() throws IOException, ParseException {
    System.out.println("test3SimpleColumns");
    Schema schema = Schema.parse(" abc, def   , ghijk");
    Assert.assertEquals(3, schema.getColumns().length);
    Assert.assertEquals("abc", schema.getColumns()[0]);
    Assert.assertEquals("def", schema.getColumns()[1]);
    Assert.assertEquals("ghijk", schema.getColumns()[2]);
  }

  @Test
  public void testNormalizeSchema() throws IOException, ParseException {
    System.out.println("testNormalizeSchema");
    String norm = Schema.normalize(" abc ,def ,   ghijk ");
    Assert.assertEquals("abc,def,ghijk", norm);
  }
}
