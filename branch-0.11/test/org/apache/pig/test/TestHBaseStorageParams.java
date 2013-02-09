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
package org.apache.pig.test;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class TestHBaseStorageParams {

    private static final Log LOG = LogFactory.getLog(TestHBaseStorageParams.class);

    @BeforeClass
    public static void setUp() throws Exception {
      // This is needed by HBaseStorage
      UDFContext.getUDFContext().setClientSystemProps(new Properties());
    }

    @Test
    public void testColumnParsingWithSpaces1() throws IOException, ParseException {
      HBaseStorage storage = new HBaseStorage("foo:a foo:b foo:c");
      doColumnParseTest(storage, "foo:a", "foo:b", "foo:c");
    }

    @Test
    public void testColumnParsingWithSpaces2() throws IOException, ParseException {
      HBaseStorage storage = new HBaseStorage("foo:a foo:b  foo:c");
      doColumnParseTest(storage, "foo:a", "foo:b", "foo:c");
    }

    @Test
    public void testColumnParsingWithCommas1() throws IOException, ParseException {
      HBaseStorage storage = new HBaseStorage("foo:a,foo:b,foo:c");
      doColumnParseTest(storage, "foo:a", "foo:b", "foo:c");
    }

    @Test
    public void testColumnParsingWithCommas2() throws IOException, ParseException {
      HBaseStorage storage = new HBaseStorage("foo:a,  foo:b , foo:c");
      doColumnParseTest(storage, "foo:a", "foo:b", "foo:c");
    }

    @Test
    public void testColumnParsingWithDelim() throws IOException, ParseException {
      HBaseStorage storage = new HBaseStorage("foo:a%foo:b % foo:c,d", "-delim %");
      doColumnParseTest(storage, "foo:a", "foo:b", "foo:c,d");
    }

    @Test
    public void testColumnParsingWithDelimWithSpaces()
      throws IOException, ParseException {

      HBaseStorage storage =
        new HBaseStorage("foo:a%foo:b % foo:c,d", "-delim % -ignoreWhitespace false");
      doColumnParseTest(storage, "foo:a", "foo:b ", " foo:c,d");
    }

    private void doColumnParseTest(HBaseStorage storage, String... names) {
      Assert.assertEquals("Wrong column count",
        names.length, storage.getColumnInfoList().size());

      for (String name : names) {
        String[] cfAndName = name.split(":");
        HBaseStorage.ColumnInfo col = storage.getColumnInfoList().remove(0);

        Assert.assertEquals("Wrong CF",
          cfAndName[0], new String(col.getColumnFamily()));
        Assert.assertEquals("Wrong column descriptor",
          cfAndName[1], new String(col.getColumnName()));
        Assert.assertNull("Column prefix should be null", col.getColumnPrefix());
      }
    }
}
