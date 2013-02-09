/*
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
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.parser.ParserException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTypedMap  {
    // this string is used in Pig scripts, where '\' is the escape character.
    // "\\" has no effect on path resolution in Java, so we escape this here to
    // fix all the cases that use this to construct paths in this test.
    private String tmpDirName = Util.encodeEscape(System.getProperty("java.io.tmpdir")) + "/pigtest/tmp";

    @Before
    public void setUp() throws Exception {
        FileLocalizer.setInitialized(false);
        File tmpDir = new File(tmpDirName);
        if (tmpDir.exists()) {
            for (File tmpFile : tmpDir.listFiles()) {
                tmpFile.delete();
            }
        } else {
            tmpDir.mkdirs();
        }

    }

    @After
    public void tearDown() throws Exception {
        File tmpDir = new File(tmpDirName);
        if (tmpDir.exists()) {
            for (File tmpFile : tmpDir.listFiles()) {
                tmpFile.delete();
            }
            tmpDir.delete();
        }
    }

    @Test
    public void testSimpleLoad() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key2#2]",
                "[key#2]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/table_testSimpleLoad", input);

        String query =
            "  a = load '" + tmpDirName + "/table_testSimpleLoad' as (m:map[int]);";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("a");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[int]}");
        Iterator<Tuple> it = pig.openIterator("a");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("1"));
        Assert.assertTrue(((Map)t.get(0)).get("key2") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key2").toString().equals("2"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("2"));

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testSimpleMapKeyLookup() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key2#2]",
                "[key#2]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testSimpleMapKeyLookup", input);

        String query =
            "a = load '" + tmpDirName + "/testSimpleMapKeyLookup' as (m:map[int]);" +
            "b = foreach a generate m#'key';";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("b");
        assertEquals("Checking expected schema",sch.toString(), "{int}");
        Iterator<Tuple> it = pig.openIterator("b");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue((Integer)t.get(0)==1);

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue((Integer)t.get(0)==2);

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testSimpleMapCast() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key2#2]",
                "[key#2]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testSimpleMapCast", input);

        String query =
            "a = load '" + tmpDirName + "/testSimpleMapCast' as (m);" +
            "b = foreach a generate ([int])m;";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("b");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[int]}");
        Iterator<Tuple> it = pig.openIterator("b");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("1"));
        Assert.assertTrue(((Map)t.get(0)).get("key2") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key2").toString().equals("2"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof Integer);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("2"));

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testComplexLoad() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#{(1,2),(1,3)},134#]",
                "[key2#]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testComplexLoad", input);

        String query = "a = load '" + tmpDirName + "/testComplexLoad' as (m:map[bag{(i:int,j:int)}]);";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("a");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[{(i: int,j: int)}]}");
        Iterator<Tuple> it = pig.openIterator("a");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("134"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof DataBag);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("{(1,2),(1,3)}"));
        Assert.assertTrue(((Map)t.get(0)).get("134")==null);

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key2")==null);

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testComplexCast() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#{(1,2),(1,3)},134#]",
                "[key2#]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testComplexCast", input);

        String query = "a = load '" + tmpDirName + "/testComplexCast' as (m);" +
            "b = foreach a generate ([{(i:int,j:int)}])m;";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("b");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[{(i: int,j: int)}]}");
        Iterator<Tuple> it = pig.openIterator("b");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("134"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof DataBag);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("{(1,2),(1,3)}"));
        Assert.assertTrue(((Map)t.get(0)).get("134")==null);

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key2")==null);

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testComplexCast2() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key2#2]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testComplexCast2", input);

        String query = "a = load '" + tmpDirName + "/testComplexCast2' as (m:[int]);" +
            "b = foreach a generate ([long])m;";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("b");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[long]}");
        Iterator<Tuple> it = pig.openIterator("b");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof Long);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("1"));
        Assert.assertTrue(((Map)t.get(0)).get("key2") instanceof Long);
        Assert.assertTrue(((Map)t.get(0)).get("key2").toString().equals("2"));

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testUnTypedMap() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key2#2]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testUnTypedMap", input);

        String query = "a = load '" + tmpDirName + "/testUnTypedMap' as (m:[]);";
        Util.registerMultiLineQuery(pig, query);
        Schema sch = pig.dumpSchema("a");
        assertEquals("Checking expected schema",sch.toString(), "{m: map[]}");
        Iterator<Tuple> it = pig.openIterator("a");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.size()==1);
        Assert.assertTrue(t.get(0) instanceof Map);
        Assert.assertTrue(((Map)t.get(0)).containsKey("key"));
        Assert.assertTrue(((Map)t.get(0)).containsKey("key2"));
        Assert.assertTrue(((Map)t.get(0)).get("key") instanceof DataByteArray);
        Assert.assertTrue(((Map)t.get(0)).get("key").toString().equals("1"));
        Assert.assertTrue(((Map)t.get(0)).get("key2") instanceof DataByteArray);
        Assert.assertTrue(((Map)t.get(0)).get("key2").toString().equals("2"));

        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testOrderBy() throws IOException, ParserException {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] input = {
                "[key#1,key1#2]",
                "[key#2,key3#2]",
                "[key#11]",
        };

        Util.createInputFile(FileSystem.getLocal(new Configuration()), tmpDirName + "/testOrderBy", input);

        String query = "a = load '" + tmpDirName + "/testOrderBy' as (m:[int]);" +
            "b = foreach a generate m#'key' as b0;" +
            "c = order b by b0;";
        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("c");

        Assert.assertTrue(it.hasNext());
        Tuple t = it.next();
        Assert.assertTrue(t.toString().equals("(1)"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.toString().equals("(2)"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.toString().equals("(11)"));

        Assert.assertFalse(it.hasNext());

        // Untyped map will sort by byte type, which is different
        query = "a = load '" + tmpDirName + "/testOrderBy' as (m:[]);" +
            "b = foreach a generate m#'key' as b0;" +
            "c = order b by b0;";
        Util.registerMultiLineQuery(pig, query);
        it = pig.openIterator("c");

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.toString().equals("(1)"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.toString().equals("(11)"));

        Assert.assertTrue(it.hasNext());
        t = it.next();
        Assert.assertTrue(t.toString().equals("(2)"));
    }
}
