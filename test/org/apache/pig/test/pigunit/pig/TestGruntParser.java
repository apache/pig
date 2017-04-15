/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.test.pigunit.pig;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.apache.pig.pigunit.pig.GruntParser;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.Test;


public class TestGruntParser {
  private GruntParser parser;
  private Map<String, String> override;

  @SuppressWarnings("serial")
  @Before
  public void setUp() throws Exception {
    override = new HashMap<String, String>() {{
      put("STORE", "");
      put("DUMP", "");
    }};
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    parser = new GruntParser(new StringReader(""), pigServer, override);
  }

  @Test
  public void testRemoveStores() throws IOException {
    Assert.assertEquals("", parser.override("STORE output INTO '/path';"));

    override.remove("STORE");
    Assert.assertEquals(
        "STORE output INTO '/path';", parser.override("STORE output INTO '/path';"));
  }

  @Test
  public void testRemoveDumps() throws IOException {
    Assert.assertEquals("", parser.override("DUMP output;"));

    override.remove("DUMP");
    Assert.assertEquals("DUMP output;", parser.override("DUMP output;"));
  }

  @Test
  public void testReplaceLoad() throws IOException {
    override.put("A", "A = LOAD 'file';");
    Assert.assertEquals(
        "A = LOAD 'file';",
        parser.override("A = LOAD 'input.txt' AS (query:CHARARRAY);"));
  }

  @Test
  public void testGetStoreAlias() throws IOException {
    override.remove("STORE");
    parser.override("STORE output INTO '/path'");
    Assert.assertEquals("output", override.get("LAST_STORE_ALIAS"));
  }

  @Test
  public void testChangeRootDirectory() throws Exception {
    Assert.assertEquals(
        "A = LOAD 'input.txt';",
        parser.override("A = LOAD 'input.txt';"));

    System.setProperty("pigunit.filesystem.prefix", "/tmp/pigunit/");
    Assert.assertEquals(
        "A = LOAD '/tmp/pigunit/input.txt';",
        parser.override("A = LOAD 'input.txt';"));

    System.clearProperty("pigunit.filesystem.prefix");
    Assert.assertEquals(
        "A = LOAD 'input.txt';",
        parser.override("A = LOAD 'input.txt';"));
  }
}
