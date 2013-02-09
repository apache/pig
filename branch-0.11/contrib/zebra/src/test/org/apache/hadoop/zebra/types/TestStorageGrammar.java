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
package org.apache.hadoop.zebra.types;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.LoginException;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test projections on complicated column types.
 * 
 */
public class TestStorageGrammar {

  final static String STR_SCHEMA = "s1:bool, s2:int, s3:long, s4:double, s5:string, s6:bytes, r1:record(f1:int, f2:long), r2:record(r3:record(f3:double, f4)), m1:map(string),m2:map(map(int)), c:collection(record(f13:double, f14:double, f15:bytes)),s7:string, s8:string, s9:string, s10:string, s11:string, s12:string, s13:string, s14:string, s15:string, s16:string, s17:string, s18:string, s19:string, s20:string, s21:string, s22:string, s23:string";
  static String STR_STORAGE = null;
  final private static Configuration conf = new Configuration();
  private static Path path;
  private static FileSystem fs;
  private static String user;
  private static String group;
  private static String defaultUser;

  static {
    try {
      String command = "whoami";
      Process process = new ProcessBuilder(command).start();
      InputStream is = process.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      System.out.printf("Output of running %s is:", command);
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println("default user0:" + line);
        defaultUser = line;

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @BeforeClass
  public static void setUpOnce() throws IOException, LoginException {
    if (System.getProperty("user") == null) {
      System.setProperty("user", defaultUser);
    }
    user = System.getProperty("user");
    if (System.getProperty("group") == null) {
      System.setProperty("group", "users");
    }
    group = System.getProperty("group");
    System.out.println("uid:" + user + " gid: " + group);
    STR_STORAGE = "[s1, s2] COMPRESS BY gz SECURE BY uid:"
        + user
        + " gid:"
        + group
        + " perm:777 SERIALIZE BY pig; [m1#{a}] SERIALIZE BY pig COMPRESS BY gz SECURE BY uid:"
        + user
        + " gid:"
        + group
        + " perm:777 ; [r1.f1] SECURE BY uid:"
        + user
        + " gid:"
        + group
        + " perm:777 SERIALIZE BY pig COMPRESS BY gz ; [s3, s4, r2.r3.f3] SERIALIZE BY pig SECURE BY uid:"
        + user
        + " gid: "
        + group
        + " perm:777 COMPRESS BY gz ; [s5, s6, m2#{x|y}] compREss by gz secURe by uid:"
        + user
        + " gid:"
        + group
        + " perm:777 SerialIZE BY pig; [r1.f2, m1#{b}]; [r2.r3.f4, m2#{z}] SERIALIZE BY avro;[s7,s8] SERIALIZE BY AVRO;[s9,s10] COMPRESS BY gz ";
    System.out.println("storage: " + STR_STORAGE);
    conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
    conf.setInt("table.input.split.minSize", 64 * 1024);
    conf.set("table.output.tfile.compression", "none");
    // String STR_STORAGE1 =
    // "[s1, s2] COMPRESS BY gz SECURE BY uid:"+USER1.getUserName()+" gid:"+USER1.getGroupNames()[0]+" perm:777 SERIALIZE BY pig";
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    path = new Path(fs.getWorkingDirectory(), "TestStorageGrammar");
    System.out.println("path: " + path.toString());
    // Process p = Runtime.getRuntime().exec("rm -rf "+path.toString()+"*");

    fs = path.getFileSystem(conf);
    // drop any previous tables
    BasicTable.drop(path, conf);

    BasicTable.Writer writer = new BasicTable.Writer(path, STR_SCHEMA,
        STR_STORAGE, conf);

    writer.finish();

    Schema schema = writer.getSchema();
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = null;
    try {
      inserter = writer1.getInserter("part" + part, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    TypesUtils.resetTuple(tuple);
    Tuple tupRecord1 = null;
    try {
      tupRecord1 = TypesUtils.createTuple(schema.getColumnSchema("r1")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
    }

    Tuple tupRecord2 = null;
    try {
      tupRecord2 = TypesUtils.createTuple(schema.getColumnSchema("r2")
          .getSchema());
    } catch (ParseException e) {
      e.printStackTrace();
    }

    Tuple tupRecord3 = null;
    try {
      tupRecord3 = TypesUtils.createTuple(new Schema("f3:float, f4"));
    } catch (ParseException e) {
      e.printStackTrace();
    }

    // row 1
    tuple.set(0, true); // bool
    tuple.set(1, 1); // int
    tuple.set(2, 1001L); // long
    tuple.set(3, 1.1); // float
    tuple.set(4, "hello world 1"); // string
    tuple.set(5, new DataByteArray("hello byte 1")); // byte

    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 1);
    tupRecord1.set(1, 1001L);
    tuple.set(6, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 1.3);
    tupRecord3.set(1, new DataByteArray("r3 row 1 byte array "));
    tuple.set(7, tupRecord2);

    // m1:map(string)
    Map<String, String> m1 = new HashMap<String, String>();
    m1.put("a", "A");
    m1.put("b", "B");
    m1.put("c", "C");
    tuple.set(8, m1);

    // m2:map(map(int))
    HashMap<String, Map> m2 = new HashMap<String, Map>();
    Map<String, Integer> m3 = new HashMap<String, Integer>();
    m3.put("m311", 311);
    m3.put("m321", 321);
    m3.put("m331", 331);
    Map<String, Integer> m4 = new HashMap<String, Integer>();
    m4.put("m411", 411);
    m4.put("m421", 421);
    m4.put("m431", 431);
    m2.put("x", m3);
    m2.put("y", m4);
    tuple.set(9, m2);

    // c:collection(f13:double, f14:float, f15:bytes)
    DataBag bagColl = TypesUtils.createBag();
    Schema schColl = schema.getColumn(10).getSchema().getColumn(0).getSchema();
    Tuple tupColl1 = TypesUtils.createTuple(schColl);
    Tuple tupColl2 = TypesUtils.createTuple(schColl);
    byte[] abs1 = new byte[3];
    byte[] abs2 = new byte[4];
    tupColl1.set(0, 3.1415926);
    tupColl1.set(1, 1.6);
    abs1[0] = 11;
    abs1[1] = 12;
    abs1[2] = 13;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 123.456789);
    tupColl2.set(1, 100);
    abs2[0] = 21;
    abs2[1] = 22;
    abs2[2] = 23;
    abs2[3] = 24;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(10, bagColl);

    // set s7 to s23
    for (int i = 7; i <= 23; i++) {
      tuple.set(i + 4, "s" + "i" + ", line1");
    }

    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    TypesUtils.resetTuple(tupRecord1);
    TypesUtils.resetTuple(tupRecord2);
    TypesUtils.resetTuple(tupRecord3);
    m1.clear();
    m2.clear();
    m3.clear();
    m4.clear();
    tuple.set(0, false);
    tuple.set(1, 2); // int
    tuple.set(2, 1002L); // long
    tuple.set(3, 3.1); // float
    tuple.set(4, "hello world 2"); // string
    tuple.set(5, new DataByteArray("hello byte 2")); // byte

    // r1:record(f1:int, f2:long
    tupRecord1.set(0, 2);
    tupRecord1.set(1, 1002L);
    tuple.set(6, tupRecord1);

    // r2:record(r3:record(f3:float, f4))
    tupRecord2.set(0, tupRecord3);
    tupRecord3.set(0, 2.3);
    tupRecord3.set(1, new DataByteArray("r3 row2  byte array"));
    tuple.set(7, tupRecord2);

    // m1:map(string)
    m1.put("a2", "A2");
    m1.put("b2", "B2");
    m1.put("c2", "C2");
    tuple.set(8, m1);

    // m2:map(map(int))
    m3.put("m321", 321);
    m3.put("m322", 322);
    m3.put("m323", 323);
    m2.put("z", m3);
    tuple.set(9, m2);

    // c:collection(f13:double, f14:float, f15:bytes)
    bagColl.clear();
    TypesUtils.resetTuple(tupColl1);
    TypesUtils.resetTuple(tupColl2);
    tupColl1.set(0, 7654.321);
    tupColl1.set(1, 0.0001);
    abs1[0] = 31;
    abs1[1] = 32;
    abs1[2] = 33;
    tupColl1.set(2, new DataByteArray(abs1));
    bagColl.add(tupColl1);
    tupColl2.set(0, 0.123456789);
    tupColl2.set(1, 0.3333);
    abs2[0] = 41;
    abs2[1] = 42;
    abs2[2] = 43;
    abs2[3] = 44;
    tupColl2.set(2, new DataByteArray(abs2));
    bagColl.add(tupColl2);
    tuple.set(10, bagColl);
    // set s7 to s23
    for (int i = 7; i <= 23; i++) {
      tuple.set(i + 4, "s" + "i" + ", line2");
    }
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    inserter.close();
    writer1.finish();

    writer.close();
  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  // @Test
  /*
   * group1 is user specific
   */
  public void test1() throws IOException, ParseException {

    String schema = "s1:string, s2:string";
    String storage = "[s1, s2]COMPRESS BY gz SECURE BY uid:user1 gid:users perm:744 SERIALIZE BY pig";
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    fs = new LocalFileSystem(rawLFS);
    Path path1 = new Path(path.toString() + "1");
    Runtime.getRuntime().exec("rm -rf " + path1.toString());

    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo ===========");
    BasicTable.dumpInfo(path1.toString(), ps, conf);

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s1:string,s2:string"));
  }

  @Test
  public void test2() throws IOException, ParseException {
    String schema = "m1:map(string),m2:map(map(int))";
    String storage = "[m1#{a}] SERIALIZE BY pig COMPRESS BY gz SECURE BY uid:"
        + user + " gid:" + group + " perm:770 ";
    Path path1 = new Path(path.toString() + "2");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);

    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : m1:map(string),m2:map(map(int))"));
  }

  @Test
  public void test3() throws IOException, ParseException {
    String schema = "r1:record(f1:int, f2:long)";
    String storage = "[r1.f1] SECURE BY uid:" + user + " gid:" + group
        + " perm:777 SERIALIZE BY pig COMPRESS BY gz";
    Path path1 = new Path(path.toString() + "3");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    // Assert.assertEquals(true,bos.toString().contains("Schema : r1:record(f1:int, f2:long)"));
  }

  @Test
  public void test4() throws IOException, ParseException {
    String schema = "s3:string, s4:string, r2:record(r3:record(f3:float, f4)), c:collection(record(f13:double, f14:float, f15:bytes))";
    String storage = "[s3, s4, r2.r3.f3, c] SERIALIZE BY pig SECURE BY uid:"
        + user + " gid:" + group + " perm:777 COMPRESS BY gz";
    Path path1 = new Path(path.toString() + "4");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    // Assert.assertEquals(true,bos.toString().contains("s3:string,s4:string,r2:record(r3:record(f3:float, f4)),c:collection(f13:double,f14:float,f15:bytes)"));
  }

  @Test
  public void test5() throws IOException, ParseException {
    String schema = "s5:string, s6:string, m2:map(map(int))";
    String storage = "[s5, s6, m2#{x|y}] compREss by gz secURe by uid:" + user
        + " gid:" + group + " perm:777 SerialIZE BY pig";
    Path path1 = new Path(path.toString() + "5");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    // Assert.assertEquals(true,bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s5:string,s6:string,m2:map(map(int))"));
  }

  @Test
  public void test6() throws IOException, ParseException {
    String schema = "r1:record(f1:int, f2:long),m1:map(string)";
    String storage = "[r1.f2, m1#{b}]";
    Path path1 = new Path(path.toString() + "6");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    // Assert.assertEquals(true,bos.toString().contains("Compressor: gz"));
    // Assert.assertEquals(true,bos.toString().contains("Schema : r1:record(f1:int, f2:long),m1:map(string)"));
  }

  @Test
  public void test7() throws IOException, ParseException {
    System.out.println("========7777");
    String schema = "r2:record(r3:record(f3:float, f4)),m2:map(map(int))";
    String storage = "[r2.r3.f4, m2#{z}] SERIALIZE BY avro";
    Path path1 = new Path(path.toString() + "7");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: avro"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    // Assert.assertEquals(true,bos.toString().contains("Schema : r2:record(r3:record(f3:float, f4)),m2:map(map(int))"));
  }

  @Test
  public void test8() throws IOException, ParseException {
    String schema = "s7:string, s8:string";
    String storage = "[s7,s8] SERIALIZE BY AvRO";
    Path path1 = new Path(path.toString() + "8");
    BasicTable.Writer writer = null;
    try {
      BasicTable.drop(path1, conf);
      writer = new BasicTable.Writer(path1, schema, storage, conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("start dumpinfo ===========\n" + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: AvRO"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s7:string,s8:string"));
  }

  @Test(expected = IOException.class)
  public void test9() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] SERIALIZE BY something";
    Path path1 = new Path(path.toString() + "9");
    BasicTable.Writer writer = null;

    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test10() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY gz SERIALLLLIZE BY pig";
    Path path1 = new Path(path.toString() + "10");
    BasicTable.Writer writer = null;

    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test11() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPREEEEESS BY gz SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "11");
    BasicTable.Writer writer = null;

    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test12() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY gz SECURrrrrrE BY uid:user1 gid:grop1 perm:760 SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "12");
    BasicTable.Writer writer = null;

    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test13() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS gz SECURE BY uid:user1 gid:users perm:760 SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "13");
    BasicTable.Writer writer = null;

    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test14() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY gz SECURE BY usssser:user1 gid:users perm:760 SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "14");
    
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = null;
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test15() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY gz SECURE BY uid:user1 grouuuup:group1 perm:760 SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "15");
    BasicTable.Writer writer = null;
    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test(expected = IOException.class)
  public void test16() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY gz SECURE BY uid:user1 gid:users perrrrrm:760 SERIALIZE BY pig";
    Path path1 = new Path(path.toString() + "16");
    BasicTable.Writer writer = null;
    BasicTable.drop(path1, conf);
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test
  public void test17() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] COMPRESS BY lzo";

    Path path1 = new Path(path.toString() + "17");
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 17 ===========");
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());
    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: lzo"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
  }

  // @Test
  public void test18() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] COMPRESS BY gZ";

    Path path1 = new Path(path.toString() + "18");
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 18===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));

    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
  }

  @Test(expected = IOException.class)
  public void test19() throws IOException, ParseException {
    String schema = "some1:string, some2:string";
    String storage = "[some1,some2] COMPRESS BY something";
    Path path1 = new Path(path.toString() + "19");
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = null;
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  // @Test
  /*
   * group1 is user specific
   */
  public void test20() throws IOException, ParseException {
    String schema = "s9:string, s10:string, s11:string";
    String storage = "[s9,s10] COMPRESS BY gZ; secure by uid:user1 gid:users perm:755";

    Path path1 = new Path(path.toString() + "20");
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 20===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
  }

  @Test(expected = IOException.class)
  public void test21() throws IOException, ParseException {
    String schema = "s9:string, s10:string, s11:string";
    String storage = "[s9,s10] COMPRESS BY gZ; secure by uid:user1 gid:users perm:755;secure by user:user1 gid:users perm:755";
    Path path1 = new Path(path.toString() + "16");
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = null;
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }

  @Test
  public void test22() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] secure by uid:user3";

    Path path1 = new Path(path.toString() + "22");
    BasicTable.drop(path1, conf);
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.close();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 22===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
    Assert.assertEquals(true, bos.toString().contains("Group: null"));
    Assert.assertEquals(true, bos.toString().contains("Perm: -1"));
  }

  // @Test
  /*
   * group1 is user specific
   */
  public void test23() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] secure by gid:users";

    Path path1 = new Path(path.toString() + "23");
    BasicTable.drop(path1, conf);
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 23===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
    Assert.assertEquals(true, bos.toString().contains("Group: users"));
    Assert.assertEquals(true, bos.toString().contains("Perm: -1"));
  }

  // @Test
  public void test24() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] secure by perm:755";

    Path path1 = new Path(path.toString() + "24");
    BasicTable.drop(path1, conf);
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 24===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
    Assert.assertEquals(true, bos.toString().contains("Group:"));
    Assert.assertEquals(true, bos.toString().contains("Perm: 0755"));
  }

  // @Test
  /*
   * group1 is user specific
   */
  public void test25() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] secure by uid:user1 gid:users";

    Path path1 = new Path(path.toString() + "25");
    BasicTable.drop(path1, conf);
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 25===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
    Assert.assertEquals(true, bos.toString().contains("Group: users"));
    Assert.assertEquals(true, bos.toString().contains("Perm: -1"));
  }

  // @Test
  public void test26() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] secure by perm:755 gid:users";

    Path path1 = new Path(path.toString() + "26");
    BasicTable.drop(path1, conf);
    Runtime.getRuntime().exec("rm -rf " + path1.toString());
    fs = path.getFileSystem(conf);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage,
        conf);
    writer.finish();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bos);
    System.out.println("start dumpinfo 26===========" + bos.toString());
    BasicTable.dumpInfo(path1.toString(), ps, conf);
    System.out.println("bos: " + bos.toString());

    Assert.assertEquals(true, bos.toString().contains("Serializer: pig"));
    Assert.assertEquals(true, bos.toString().contains("Compressor: gz"));
    Assert.assertEquals(true, bos.toString().contains(
        "Schema : s9:string,s10:string"));
    Assert.assertEquals(true, bos.toString().contains("Group: users"));
    Assert.assertEquals(true, bos.toString().contains("Perm: 755"));
  }

  @Test(expected = IOException.class)
  public void test27() throws IOException, ParseException {
    String schema = "s9:string, s10:string";
    String storage = "[s9,s10] COMPRESS BY gZ; secure by ";
    Path path1 = new Path(path.toString() + "27");
    BasicTable.drop(path1, conf);
    BasicTable.Writer writer = null;
    writer = new BasicTable.Writer(path1, schema, storage, conf);
    Assert.fail("should throw exception, none defined serializer");
    writer.finish();
  }
}
