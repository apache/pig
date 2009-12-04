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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.ParseException;
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
public class TestColumnSecurity {

  final private static Configuration conf = new Configuration();
  private static Path path;
  private static FileSystem fs;
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String GROUP4_NAME = "group4";
  final private static String USER1_NAME = "user1";
  final private static String USER2_NAME = "user2";
  final private static String USER3_NAME = "user3";

  private static UnixUserGroupInformation SUPERUSER;
  private static UnixUserGroupInformation USER1;
  private static UnixUserGroupInformation USER2;
  private static UnixUserGroupInformation USER3;
  private static String user;
  static {
    try {

      conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
      conf.setInt("table.input.split.minSize", 64 * 1024);
      conf.set("table.output.tfile.compression", "none");

      // Initiate all four users
      SUPERUSER = UnixUserGroupInformation.login(conf);
      USER1 = new UnixUserGroupInformation(USER1_NAME, new String[] {
          GROUP1_NAME, GROUP2_NAME });
      USER2 = new UnixUserGroupInformation(USER2_NAME, new String[] {
          GROUP2_NAME, GROUP3_NAME });
      USER3 = new UnixUserGroupInformation(USER3_NAME, new String[] {
          GROUP3_NAME, GROUP4_NAME });
      System.out.println("SUPERUSER NAME: " + SUPERUSER.getUserName());

    } catch (LoginException e) {
      throw new RuntimeException(e);
    }
    if (System.getProperty("user") == null) {
      System.setProperty("user", "jing1234");
    }
    user = System.getProperty("user");
    if (System.getProperty("group") == null) {
      System.setProperty("group", "users");
    }
    System.getProperty("group");
  }

  @BeforeClass
  public static void setUpOnce() throws IOException, LoginException {
    path = new Path("/user/" + user);
    fs = path.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));

  }

  @AfterClass
  public static void tearDownOnce() throws IOException {
  }

  /* log into dfs as the given user */
  private static void login(UnixUserGroupInformation ugi) throws IOException {
    if (fs != null) {
      fs.close();
    }
    UnixUserGroupInformation.saveToConf(conf,
        UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    fs = FileSystem.get(conf); // login as ugi
  }

  @Test
  public void test1() throws IOException, ParseException {
    /*
     * Test permission 777, everyone can read, write and delete
     */
    System.out.println("In test1...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2 ] secure by uid:user1 gid:group1 perm:777";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest1");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER2);
    load(USER2.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    load(USER3.getUserName(), path1, "s1,s2", false, false);

    login(USER1);
    BasicTable.dropColumnGroup(path1, conf, "CG0");
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (Exception e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", true, true);
    } catch (Exception e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", true, true);
    } catch (Exception e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

  }

  @Test
  public void test2() throws IOException, ParseException {
    /*
     * Test group has no read permission, but has write permission
     */
    System.out.println("In test2...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:730";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest2");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {
    }

    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName()
          + " should not be able to delete, can not open directory");

    } catch (IOException e) {

    }

    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }

    login(USER1);
    BasicTable.dropColumnGroup(path1, conf, "CG0");
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should  be able to read");

    }

  }

  @Test
  public void test3() throws IOException, ParseException {
    /*
     * Group has NO read permission, has NO write permission OTHERS has NO read
     * No write permission
     */
    System.out.println("In test3...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:700";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest3");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER1);
    BasicTable.dropColumnGroup(path1, conf, "CG0");
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

  }

  @Test
  public void test4() throws IOException, ParseException {
    /*
     * GROUP has NO write permission, but has read permission
     */
    System.out.println("In test4...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:740";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest4");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should  be able to read");
    } catch (IOException e) {
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    /*
     * should not be able to read, since there is no execute permission
     */
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER1);
    BasicTable.dropColumnGroup(path1, conf, "CG0");
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

  }

  @Test
  public void test5() throws IOException, ParseException {
    /*
     * OTHERS has NO read permission, has write permission
     */
    System.out.println("In test5...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group1 perm:702";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest5");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should  be able to read");
    } catch (IOException e) {
    }

    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should  be able to read");
    } catch (IOException e) {
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER1);
    BasicTable.dropColumnGroup(path1, conf, "CG0");
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");
    }

  }

  @Test
  public void test6() throws IOException, ParseException {
    /*
     * OTHERS has NO write permission, but has read permission
     */
    System.out.println("In test6...");
    String schema = "s1:string, s2:string";
    String storage = "[s1,s2] secure by uid:user1 gid:group1 perm:705;";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest6");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should  not  be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER1);
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should  be able to read");
    }
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should  be able to read");
    }

  }

  @Test
  public void test7() throws IOException, ParseException {
    /*
     * USER has NO read permission, but has write permission
     */
    System.out.println("In test7...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group1 perm:200";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest7");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);

    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test8() throws IOException, ParseException {
    /*
     * GROUP has both read and write permission
     */
    System.out.println("In test8...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:770; [s2]";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest8");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();
    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should NOT be able to read");
    } catch (IOException e) {

    }

    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
      // e.printStackTrace();
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should not be able to write");
    }
    try {
      load(USER2.getUserName(), path1, "s1,s2", true, false);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }

    /*
     * USER1 drop the same CG0 again, should pass, take as no-op
     */
    login(USER1);
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should  be able to delete again");
    }

    try {
      load(USER1.getUserName(), path1, "s1,s2", true, false);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should not be able to read");

    }

  }

  @Test
  public void test9() throws IOException, ParseException {
    /*
     * OTHERS has BOTH read and write permission
     */
    System.out.println("In test9...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:707";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest9");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to read");

    }
  
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {
    }

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to read");

    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to write");
    }

    login(USER1);
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, true);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  not be able to read");

    }
  }

  @Test
  public void test10() throws IOException, ParseException {
    /*
     * Negative test case on table.write. user has no write permission
     */
    System.out.println("In test10...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group1 perm:400";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest10");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);

    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test11() throws IOException, ParseException {
    /*
     * stitch 2 CGs, user has read permission on both
     */
    System.out.println("In test11...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:770;[s2] secure by user:user1 gid:group2 perm:770";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest11");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);

    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should not be able to read");
    }

  }

  @Test
  public void test12() throws IOException, ParseException {
    /*
     * stitch 2 CGs, user has read permission one one but not the other
     */
    System.out.println("In test12...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:770;[s2] secure by user:user1 gid:group2 perm:700";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest12");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }

  }

  @Test
  public void test13() throws IOException, ParseException {
    /*
     * stitch 2 CGs, user has read permission on the second CG, but not the
     * FIRST one
     */
    System.out.println("In test13...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:700;[s2] secure by user:user1 gid:group2 perm:770";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest13");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }

  }

  @Test
  public void test14() throws IOException, ParseException {
    /*
     * stitch 2CGs where user only has write permission on those
     */
    System.out.println("In test14...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:730;[s2] secure by user:user1 gid:group2 perm:730";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest14");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER2.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }

  }

  @Test
  public void test15() throws IOException, ParseException {
    /*
     * Test stitch after CGs is dropped
     */
    System.out.println("In test15...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:user1 gid:group2 perm:770;[s2] secure by user:user1 gid:group2 perm:770";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest15");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
      Assert.fail(USER3.getUserName() + " should not be able to read");
    } catch (IOException e) {

    }
    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should be able to write");
    }
    try {
      load(USER2.getUserName(), path1, "s1,s2", true, false);

    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should be able to read");
    }
  }

  @Test
  public void test16() throws IOException, ParseException {
    /*
     * stitch on non-secured CGs should not be affected
     */
    System.out.println("In test16...");
    String schema = "s1:string, s2:string";
    String storage = "[s1];[s2]";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest16");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(USER1.getUserName(), schema1, path1);
    writer.close();
    load(USER1.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should be able to read");
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER2.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should be able to read");
    }

    login(USER1);
    try {
      load(USER1.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should not be able to write");

    }
    try {
      load(USER1.getUserName(), path1, "s1,s2", true, false);
    } catch (IOException e) {
      Assert.fail(USER1.getUserName() + " should be able to read");

    }
  }

  @Test
  public void test17() throws IOException, ParseException {
    /*
     * Negative test case on table.writer.Nobody has write permission
     */
    System.out.println("In test17...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group1 perm:000";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest17");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);

    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test18() throws IOException, ParseException {
    /*
     * Negative test case on table.write. Group does not exists
     */
    System.out.println("In test18...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:nonexisting perm:000";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest18");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);

    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test19() throws IOException, ParseException {
    /*
     * Negative test case on table.write. Permission setting is wrong
     */
    System.out.println("In test19...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:880";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest19");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);
    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test20() throws IOException, ParseException {
    /*
     * Negative test case on table.write. Permission setting is wrong
     */
    System.out.println("In test120...");
    String schema = "s1:string, s2:string";
    String storage = "[s1, s2] secure by uid:user1 gid:group2 perm:7777";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest20");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));
    login(USER1);

    try {
      new BasicTable.Writer(path1, schema, storage, conf);
      Assert.fail("write should fail");
    } catch (IOException e) {
    }

  }

  @Test
  public void test21() throws IOException, ParseException {
    /*
     * test on SUPERGROUP
     */
    System.out.println("In test21...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:"
        + SUPERUSER.getUserName()
        + " gid:supergroup perm:755;[s2] secure by uid:user1 gid:group2 perm:755";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest21");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));

    login(SUPERUSER);
    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(SUPERUSER.getUserName(), schema1, path1);
    writer.close();

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to read");
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }

    login(USER1);
    try {
      load(USER1.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }

    login(SUPERUSER);
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(SUPERUSER.getUserName() + " shoul be able to write");
    }
    try {
      load(SUPERUSER.getUserName(), path1, "s1,s2", true, false);
    } catch (IOException e) {
      Assert.fail(SUPERUSER.getUserName() + " should not be able to read");

    }
  }

  @Test
  public void test22() throws IOException, ParseException {
    /*
     * test on SUPERGROUP
     */
    System.out.println("In test22...");
    String schema = "s1:string, s2:string";
    String storage = "[s1] secure by uid:"
        + SUPERUSER.getUserName()
        + " gid:supergroup perm:775;[s2] secure by uid:user1 gid:group2 perm:775";
    Path path1 = new Path(path.toString() + "/TestColumnStorageTest22");
    login(SUPERUSER);
    fs = path1.getFileSystem(conf);
    fs.setPermission(path, new FsPermission((short) 0777));

    BasicTable.Writer writer = new BasicTable.Writer(path1, schema, storage, conf);
    writer.finish();

    Schema schema1 = writer.getSchema();
    insertData(SUPERUSER.getUserName(), schema1, path1);
    writer.close();
    load(SUPERUSER.getUserName(), path1, "s1,s2", false, false);

    login(USER3);
    try {
      load(USER3.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should  be able to read");
    }

    login(USER2);
    try {
      load(USER2.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }

    login(USER1);
    try {
      load(USER1.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER2.getUserName() + " should  be able to read");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
      Assert.fail(USER3.getUserName() + " should not be able to write");
    } catch (IOException e) {
    }
    try {
      load(USER1.getUserName(), path1, "s1,s2", false, false);
    } catch (IOException e) {
      Assert.fail(USER3.getUserName() + " should not be able to write");
    }

    login(SUPERUSER);
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(SUPERUSER.getUserName() + " shoul be able to delete ");
    }
    try {
      BasicTable.dropColumnGroup(path1, conf, "CG0");
    } catch (IOException e) {
      Assert.fail(SUPERUSER.getUserName() + " shoul be able to delete agains");
    }
    try {
      load(SUPERUSER.getUserName(), path1, "s1,s2", true, false);
    } catch (IOException e) {
      Assert.fail(SUPERUSER.getUserName() + " should not be able to read");

    }
  }

  // set date for schema like "s1:string, s2:string"
  void insertData(String myuser, Schema schema, Path path) throws IOException {
    System.out.println(myuser + " is inserting table...");
    Tuple tuple = TypesUtils.createTuple(schema);
    BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
    int part = 0;
    TableInserter inserter = writer1.getInserter("part" + part, true);
    TypesUtils.resetTuple(tuple);

    // row 1
    tuple.set(0, "column1_1");
    tuple.set(1, "column2_1");
    int row = 0;
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);

    // row 2
    row++;
    TypesUtils.resetTuple(tuple);
    tuple.set(0, "column1_2");
    tuple.set(1, "column2_2");
    inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
        .getBytes()), tuple);
    inserter.close();
    writer1.finish();
  }

  // load projection "s1:string, s2:string"
  public void load(String myuser, Path path, String projection,
      boolean s1dropped, boolean s2dropped) throws IOException, ParseException {
    System.out.println(myuser + "is reading....");
    BasicTable.Reader reader = new BasicTable.Reader(path, conf);
    reader.setProjection(projection);
    List<RangeSplit> splits = reader.rangeSplit(1);
    TableScanner scanner = reader.getScanner(splits.get(0), true);
    BytesWritable key = new BytesWritable();
    Tuple RowValue = TypesUtils.createTuple(scanner.getSchema());

    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
    scanner.getValue(RowValue);
    if (s1dropped == true) {
      Assert.assertEquals(null, RowValue.get(0));
    } else {
      Assert.assertEquals("column1_1", RowValue.get(0));
    }
    if (s2dropped == true) {
      Assert.assertEquals(null, RowValue.get(1));
    } else {
      Assert.assertEquals("column2_1", RowValue.get(1));
    }
    scanner.advance();
    scanner.getKey(key);
    // Assert.assertEquals(key, new BytesWritable("k12".getBytes()));
    scanner.getValue(RowValue);
    if (s1dropped == true) {
      Assert.assertEquals(null, RowValue.get(0));
    } else {
      Assert.assertEquals("column1_2", RowValue.get(0));
    }
    if (s2dropped == true) {
      Assert.assertEquals(null, RowValue.get(1));
    } else {
      Assert.assertEquals("column2_2", RowValue.get(1));
    }

  }
}
