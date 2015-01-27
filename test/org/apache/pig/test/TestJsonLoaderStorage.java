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

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class TestJsonLoaderStorage {

  private static final String schema =
    "a:boolean," +
    "b:int," +
    "c:long," +
    "d:float," +
    "e:double," +
    "f:datetime," +
    "g:bytearray," +
    "h:chararray," +
    "i:biginteger," +
    "j:bigdecimal," +
    "k:map[chararray]," +
    "l:tuple(a:int)," +
    "m:bag{a:tuple(a:int)}";

  private static final String rawInput =
    "true\t" +
    "123\t" +
    "456\t" +
    "7.89\t" +
    "0.12\t" +
    "2013-01-02T03:04:05.123+03:00\t" +
    "abc\t" +
    "def\t" +
    "123456789\t" +
    "1234.6789\t" +
    "[a#ghi]\t" +
    "(123)\t" +
    "{(123),(456),(789)}";

  private static final String nullInput =
    "\t\t\t\t\t\t\t\t\t\t\t\t";

  private static final String json =
    "{" +
    "\"a\":true," +
    "\"b\":123," +
    "\"c\":456," +
    "\"d\":7.89," +
    "\"e\":0.12," +
    "\"f\":\"2013-01-02T03:04:05.123+03:00\"," +
    "\"g\":\"abc\"," +
    "\"h\":\"def\"," +
    "\"i\":123456789," +
    "\"j\":1234.6789," +
    "\"k\":{\"a\":\"ghi\"}," +
    "\"l\":{\"a\":123}," +
    "\"m\":[{\"a\":123},{\"a\":456},{\"a\":789}]" +
    "}";

  private static final String arraysJson =
    "{" +
    "\"s\":[\"abc\",\"def\",\"ghi\"]," +
    "\"i\":[23,45,78]," +
    "\"f\":[23.1,45.2,78.3]" +
    "}";

  private static final String nullJson =
    "{" +
    "\"a\":null," +
    "\"b\":null," +
    "\"c\":null," +
    "\"d\":null," +
    "\"e\":null," +
    "\"f\":null," +
    "\"g\":null," +
    "\"h\":null," +
    "\"i\":null," +
    "\"j\":null," +
    "\"k\":null," +
    "\"l\":null," +
    "\"m\":null" +
    "}";
  
  private static final String bigDecimalJson =
    "{" +
    "\"a\":123.456," +
    "\"b\":\"123.456\"" +
    "}";
	
  private static final String badJson =
    "{" +
    "\"a\":\"good\"," +
    "\"b\":\"good\"" +
    "}\n" +
	"{" +
    "\"a\":bad," +
    "\"b\":\"good\"" +
    "}\n" +
	"{" +
    "\"a\":\"good\"," +
    "\"b\":\"good\"" +
    "}";
      
  private static final String jsonOutput =
    "{\"f1\":\"18\",\"count\":3}";

  private PigServer pigServer;

  @Before
  public void setup() throws Exception {
    pigServer = new PigServer(Util.getLocalTestMode());
  }

  private String getTempOutputPath() throws IOException {
    File tempFile = File.createTempFile("json", null);
    tempFile.delete();
    tempFile.deleteOnExit();

    String path = tempFile.getAbsolutePath();
    if (Util.WINDOWS) {
      path = path.replace('\\', '/');
    }
    return path;
  }

  private String createInput(String input) throws IOException {
    File tempInputFile = File.createTempFile("input", null);
    tempInputFile.deleteOnExit();

    FileWriter w = new FileWriter(tempInputFile);
    w.write(input);
    w.close();
    String pathInputFile = tempInputFile.getAbsolutePath();
    if (Util.WINDOWS) {
      pathInputFile = pathInputFile.replace('\\', '/');
    }
    return pathInputFile;
  }

  private Iterator<Tuple> loadJson(String input) throws IOException {
    String path = createInput(input);
    pigServer.registerQuery("data = load '" + path
        + "' using JsonLoader('" + schema + "');");

    return pigServer.openIterator("data");
  }

  private BufferedReader storeJson(String input) throws Exception {
    String pathInputFile = createInput(input);
    String pathJsonFile = getTempOutputPath();
    pigServer.registerQuery("data = load '" + pathInputFile
        + "' as (" + schema + ");");
    pigServer.registerQuery("store data into '" + pathJsonFile
        + "' using JsonStorage();");

    Path p = new Path(pathJsonFile);
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    Reader r = new InputStreamReader(fs.open(Util.getFirstPartFile(p)));

    BufferedReader br = new BufferedReader(r);

    return br;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testJsonLoader() throws IOException {
    Iterator<Tuple> tuples = loadJson(json);

    int count = 0;
    while(tuples.hasNext()) {
      Tuple tuple = tuples.next();
      assertEquals(Boolean.class, tuple.get(0).getClass());
      assertEquals(Integer.class, tuple.get(1).getClass());
      assertEquals(Long.class, tuple.get(2).getClass());
      assertEquals(Float.class, tuple.get(3).getClass());
      assertEquals(Double.class, tuple.get(4).getClass());

      assertEquals(DateTime.class, tuple.get(5).getClass());
      assertEquals("2013-01-02T03:04:05.123+03:00", tuple.get(5).toString());

      assertEquals(DataByteArray.class, tuple.get(6).getClass());
      assertEquals(String.class, tuple.get(7).getClass());
      assertEquals(BigInteger.class, tuple.get(8).getClass());
      assertEquals(BigDecimal.class, tuple.get(9).getClass());

      assertTrue(tuple.get(10) instanceof Map);
      assertEquals(String.class, ((Map)tuple.get(10)).get("a").getClass());

      assertTrue(tuple.get(11) instanceof Tuple);
      assertEquals(Integer.class, ((Tuple)tuple.get(11)).get(0).getClass());

      assertTrue(tuple.get(12) instanceof DataBag);

      DataBag bag = (DataBag)tuple.get(12);
      assertEquals(3, bag.size());

      Iterator<Tuple> bagTuples = bag.iterator();
      while(bagTuples.hasNext()) {
        assertEquals(Integer.class, bagTuples.next().get(0).getClass());
      }

      count++;
    }

    assertEquals(1, count);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testJsonLoaderBadRow() throws IOException{

    String badJsonFile = createInput(badJson);
    pigServer.registerQuery("data = load '" + badJsonFile + "' using JsonLoader('a:chararray, b:chararray');");
    Iterator<Tuple> tuples = pigServer.openIterator("data");
    
    Tuple t = tuples.next();
    assertTrue(t.size()==2);
    assertTrue(t.get(0)!=null);
    assertTrue(t.get(1)!=null);
    assertTrue(tuples.hasNext());

    // bad row - skip it, returning a null tuple.
    t = tuples.next();
    assertTrue(t.size()==2);
    assertTrue(t.get(0)==null);
    assertTrue(t.get(1)==null);
    assertTrue(tuples.hasNext());

    t = tuples.next();
    assertTrue(t.size()==2);
    assertTrue(t.get(0)!=null);
    assertTrue(t.get(1)!=null);
    assertTrue(!tuples.hasNext());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testJsonLoaderArrays() throws IOException{

    String arraysJsonFile = createInput(arraysJson);
    pigServer.registerQuery("data = load '" + arraysJsonFile + "' using JsonLoader('s:bag{a:tuple(a:chararray)}, i:bag{a:tuple(a:int)}, f:bag{a:tuple(a:double)}');");

    Iterator<Tuple> tuples = pigServer.openIterator("data");
    
    Tuple t = tuples.next();
    assertTrue(t.size()==3);
    assertTrue(t.get(0)!=null);
    assertTrue(t.get(1)!=null);
    assertTrue(t.get(2)!=null);
    assertTrue(! tuples.hasNext());

  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testJsonLoaderBigDecimalFormats() throws IOException{

    String bigDecimalJsonFile = createInput(bigDecimalJson);
    pigServer.registerQuery("data = load '" + bigDecimalJsonFile + "' using JsonLoader('a:bigdecimal, b:bigdecimal');");
    Iterator<Tuple> tuples = pigServer.openIterator("data");
    
    Tuple t = tuples.next();
    assertTrue(t.size()==2);
    assertTrue(t.get(0)!=null);
    assertTrue(t.get(1)!=null);
    assertEquals(t.get(0), t.get(1));
    assertTrue(!tuples.hasNext());
  }
  
  @Test
  public void testJsonLoaderNull() throws IOException {
    Iterator<Tuple> tuples = loadJson(nullJson);

    int count = 0;
    while(tuples.hasNext()) {
      Tuple tuple = tuples.next();

      assertEquals(null, tuple.get(0));
      assertEquals(null, tuple.get(1));
      assertEquals(null, tuple.get(2));
      assertEquals(null, tuple.get(3));
      assertEquals(null, tuple.get(4));
      assertEquals(null, tuple.get(5));
      assertEquals(null, tuple.get(6));
      assertEquals(null, tuple.get(7));
      assertEquals(null, tuple.get(8));
      assertEquals(null, tuple.get(9));
      assertEquals(null, tuple.get(10));
      assertEquals(null, tuple.get(11));
      assertEquals(null, tuple.get(12));

      count++;
    }

    assertEquals(1, count);
  }

  @Test
  public void testJsonStorage() throws Exception {
    BufferedReader br = storeJson(rawInput);
    String data = br.readLine();

    assertEquals(json, data);

    String line = data;
    int count = 0;
    while (line != null) {
      line = br.readLine();
      count++;
    }
    assertEquals(1, count);

    br.close();
  }

  @Test
  public void testJsonStorageNull() throws Exception {
    BufferedReader br = storeJson(nullInput);
    String data = br.readLine();

    assertEquals(nullJson, data);

    String line = data;
    int count = 0;
    while (line != null) {
      line = br.readLine();
      count++;
    }
    assertEquals(1, count);

    br.close();
  }

  @Test
  public void testJsonLoaderStorage() throws Exception {

    String pattInputFile = createInput(rawInput);
    String pattJsonFile = getTempOutputPath();
    String pattJson2File = getTempOutputPath();

    pigServer.registerQuery("data = load '" + pattInputFile
        + "' as (" + schema + ");");
    pigServer.registerQuery("store data into '" + pattJsonFile
        + "' using JsonStorage();");
    pigServer.registerQuery("json = load '" + pattJsonFile
        + "' using JsonLoader('" + schema + "');");
    pigServer.registerQuery("store json into '" + pattJson2File
        + "' using JsonStorage();");

    Path p = new Path(pattJson2File);
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    Reader r = new InputStreamReader(fs.open(Util.getFirstPartFile(p)));

    BufferedReader br = new BufferedReader(r);
    String data = br.readLine();

    assertEquals(json, data);

    String line = data;
    int count = 0;
    while (line != null) {
      line = br.readLine();
      count++;
    }
    assertEquals(1, count);

    br.close();
  }

  @Test
  public void testJsonStorageLimit() throws Exception {
    String outPath = getTempOutputPath();
    Data data = Storage.resetData(pigServer);
    data.set("foo", tuple(1), tuple(2), tuple(3), tuple(4));
    pigServer.registerQuery("data = load 'foo' using mock.Storage() as (id:int);");
    pigServer.registerQuery("data = order data by id;");
    pigServer.registerQuery("data = limit data 2;");
    pigServer.registerQuery("store data into '" + outPath + "' using JsonStorage();");

    Path p = new Path(outPath);
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    Reader r = new InputStreamReader(fs.open(Util.getFirstPartFile(p)));

    BufferedReader br = new BufferedReader(r);

    String line = null;
    int count = 0;
    while ((line = br.readLine()) != null) {
      count++;
      assertEquals("{\"id\":" + count + "}", line);
    }
    assertEquals(2, count);

    br.close();
  }

  @Test
  public void testSimpleMapSideStreaming() throws Exception {
    File input = Util.createInputFile("tmp", "", new String [] {"1,2,3;4,5,6,7,8",
        "1,2,3;4,5,6,7,9",
        "1,2,3;4,5,6,7,18"});
    File tempJsonFile = File.createTempFile("json", "");
    tempJsonFile.delete();

    // Pig query to run
    pigServer.registerQuery("IP = load '"+  Util.generateURI(input.toString(), pigServer.getPigContext())
        +"' using PigStorage (';') as (ID:chararray,DETAILS:chararray);");
    pigServer.registerQuery(
        "id_details = FOREACH IP GENERATE " +
            "FLATTEN" +
            "(STRSPLIT" +
            "(ID,',',3)) AS (drop, code, transaction) ," +
            "FLATTEN" +
            "(STRSPLIT" +
            "(DETAILS,',',5)) AS (lname, fname, date, price, product);");
    pigServer.registerQuery(
        "transactions = FOREACH id_details GENERATE $0 .. ;");
    pigServer.registerQuery(
        "transactionsG = group transactions by code;");
    pigServer.registerQuery(
        "uniqcnt  = foreach transactionsG {"+
            "sym = transactions.product ;"+
            "dsym =  distinct sym ;"+
            "generate flatten(dsym.product) as f1, COUNT(dsym) as count ;" +
            "};");
    pigServer.store("uniqcnt", tempJsonFile.getAbsolutePath(), "JsonStorage");

    Path p = new Path(tempJsonFile.getAbsolutePath());
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    Reader r = new InputStreamReader(fs.open(Util.getFirstPartFile(p)));

    BufferedReader br = new BufferedReader(r);
    String data = br.readLine();

    assertEquals(jsonOutput, data);

    String line = data;
    int count = 0;
    while (line != null) {
      line = br.readLine();
      count++;
    }
    assertEquals(3, count);

    br.close();
    tempJsonFile.deleteOnExit();
  }

}
