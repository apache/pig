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

package org.apache.hadoop.zebra.mapred;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.zebra.mapred.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapred.TestBasicTableIOFormatLocalFS.InvIndex;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.ZebraTuple;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a sample a complete MR sample code for Table. It doens't contain
 * 'read' part. But, it should be similar and easier to write. Refer to test
 * cases in the same directory.
 * 
 * Assume the input files contain rows of word and count, separated by a space:
 * 
 * <pre>
 * us 2
 * japan 2
 * india 4
 * us 2
 * japan 1
 * india 3
 * nouse 5
 * nowhere 4
 * 
 * 
 */
public class TestMultipleOutputsTypeApi extends Configured implements Tool {

  static String inputPath;
  static String inputFileName = "multi-input.txt";
  //protected static ExecType execType = ExecType.MAPREDUCE;
  protected static ExecType execType = ExecType.LOCAL;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  // private static Path pathWorking, pathTable1, path2, path3,
  // pathTable4, pathTable5;
  private static Configuration conf;
  public static String sortKey = null;

  private static FileSystem fs;

  private static String zebraJar;
  private static String whichCluster;
  private static String multiLocs;
  private static String strTable1 = null;
  private static String strTable2 = null;
  private static String strTable3 = null;

  @BeforeClass
  public static void setUpOnce() throws IOException {
    if (System.getenv("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }

    // by default we use miniCluster
    if (System.getenv("whichCluster") == null) {
      whichCluster = "miniCluster";
    } else {
      whichCluster = System.getenv("whichCluster");
    }

    if (conf == null) {
      conf = new Configuration();
    }
    
    if (whichCluster.equals("realCluster")) {
      System.out.println(" get env hadoop home: " + System.getenv("HADOOP_HOME"));
      System.out.println(" get env user name: " + System.getenv("USER"));
      
      if (System.getenv("HADOOP_HOME") == null) {
        System.out.println("Please set HADOOP_HOME for realCluster testing mode");
        System.exit(0);        
      }
      
      if (System.getenv("USER") == null) {
        System.out.println("Please set USER for realCluster testing mode");
        System.exit(0);        
      }
      
      zebraJar = System.getenv("HADOOP_HOME") + "/lib/zebra.jar";

      File file = new File(zebraJar);
      if (!file.exists()) {
        System.out.println("Please place zebra.jar at $HADOOP_HOME/lib");
        System.exit(0);
      }
    }
    
    // set inputPath and output path
    String workingDir = null;
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      inputPath = new String("/user/" + System.getenv("USER") + "/"
          + inputFileName);
      System.out.println("inputPath: " + inputPath);
      multiLocs = new String("/user/" + System.getenv("USER") + "/" + "us"
          + "," + "/user/" + System.getenv("USER") + "/" + "india" + ","
          + "/user/" + System.getenv("USER") + "/" + "japan");
      fs = new Path(inputPath).getFileSystem(conf);

    } else {
      RawLocalFileSystem rawLFS = new RawLocalFileSystem();
      fs = new LocalFileSystem(rawLFS);
      workingDir = fs.getWorkingDirectory().toString().split(":")[1];
      inputPath = new String(workingDir + "/" + inputFileName);
      System.out.println("inputPath: " + inputPath);
      multiLocs = new String(workingDir + "/" + "us" + "," + workingDir + "/"
          + "india" + "," + workingDir + "/" + "japan");
    }
    writeToFile(inputPath);
    // check inputPath existence
    File inputFile = new File(inputPath);
    if (!inputFile.exists() && whichCluster.equalsIgnoreCase("realCluster")) {
      System.out.println("Please put inputFile in hdfs: " + inputPath);
      // System.exit(0);
    }
    if (!inputFile.exists() && whichCluster.equalsIgnoreCase("miniCluster")) {
      System.out
          .println("Please put inputFile under workingdir. working dir is : "
              + workingDir);
      System.exit(0);
    }

    if (whichCluster.equalsIgnoreCase("realCluster")) {
      pigServer = new PigServer(ExecType.MAPREDUCE, ConfigurationUtil
          .toProperties(conf));
      pigServer.registerJar(zebraJar);

    }

    if (whichCluster.equalsIgnoreCase("miniCluster")) {
      if (execType == ExecType.MAPREDUCE) {
        cluster = MiniCluster.buildCluster();
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        fs = cluster.getFileSystem();

      } else {
        pigServer = new PigServer(ExecType.LOCAL);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (whichCluster.equalsIgnoreCase("miniCluster")) {
      pigServer.shutdown();
    }
  }

  public String getCurrentMethodName() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    (new Throwable()).printStackTrace(pw);
    pw.flush();
    String stackTrace = baos.toString();
    pw.close();

    StringTokenizer tok = new StringTokenizer(stackTrace, "\n");
    tok.nextToken(); // 'java.lang.Throwable'
    tok.nextToken(); // 'at ...getCurrentMethodName'
    String l = tok.nextToken(); // 'at ...<caller to getCurrentRoutine>'
    // Parse line 3
    tok = new StringTokenizer(l.trim(), " <(");
    String t = tok.nextToken(); // 'at'
    t = tok.nextToken(); // '...<caller to getCurrentRoutine>'
    StringTokenizer st = new StringTokenizer(t, ".");
    String methodName = null;
    while (st.hasMoreTokens()) {
      methodName = st.nextToken();
    }
    return methodName;
  }

  public static void writeToFile(String inputFile) throws IOException {
    if (whichCluster.equalsIgnoreCase("miniCluster")) {
      FileWriter fstream = new FileWriter(inputFile);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write("us 2\n");
      out.write("japan 2\n");
      out.write("india 4\n");
      out.write("us 2\n");
      out.write("japan 1\n");
      out.write("india 3\n");
      out.write("nouse 5\n");
      out.write("nowhere 4\n");
      out.close();
    }
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      FSDataOutputStream fout = fs.create(new Path(inputFile));
      fout.writeBytes("us 2\n");
      fout.writeBytes("japan 2\n");
      fout.writeBytes("india 4\n");
      fout.writeBytes("us 2\n");
      fout.writeBytes("japan 1\n");
      fout.writeBytes("india 3\n");
      fout.writeBytes("nouse 5\n");
      fout.writeBytes("nowhere 4\n");
      fout.close();
    }
  }

  public Path generateOutPath(String currentMethod) {
    Path outPath = null;
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      outPath = new Path("/user/" + System.getenv("USER") + "/multiOutput/"
          + currentMethod);
    } else {
      String workingDir = fs.getWorkingDirectory().toString().split(":")[1];
      outPath = new Path(workingDir + "/multiOutput/" + currentMethod);
      System.out.println("output file: " + outPath.toString());
    }
    return outPath;
  }

  public void removeDir(Path outPath) throws IOException {
    String command = null;
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      command = System.getenv("HADOOP_HOME") + "/bin/hadoop fs -rmr "
          + outPath.toString();
    } else {
      StringTokenizer st = new StringTokenizer(outPath.toString(), ":");
      int count = 0;
      String file = null;
      while (st.hasMoreElements()) {
        count++;
        String token = st.nextElement().toString();
        if (count == 2)
          file = token;
      }
      command = "rm -rf " + file;
    }
    Runtime runtime = Runtime.getRuntime();
    Process proc = runtime.exec(command);
    int exitVal = -1;
    try {
      exitVal = proc.waitFor();
    } catch (InterruptedException e) {
      System.err.println(e);
    }

  }

  public static void getTablePaths(String myMultiLocs) {
    StringTokenizer st = new StringTokenizer(myMultiLocs, ",");

    // get how many tokens inside st object
    System.out.println("tokens count: " + st.countTokens());
    int count = 0;

    // iterate st object to get more tokens from it
    while (st.hasMoreElements()) {
      count++;
      String token = st.nextElement().toString();
      if (count == 1)
        strTable1 = token;
      if (count == 2)
        strTable2 = token;
      if (count == 3)
        strTable3 = token;
      System.out.println("token = " + token);
    }
  }

  public static void checkTable(String myMultiLocs) throws IOException {
    System.out.println("myMultiLocs:" + myMultiLocs);
    System.out.println("sorgetTablePathst key:" + sortKey);

    getTablePaths(myMultiLocs);
    String query1 = null;
    String query2 = null;
    String query3 = null;

    if (strTable1 != null) {

      query1 = "records1 = LOAD '" + strTable1
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    }
    if (strTable2 != null) {
      query2 = "records2 = LOAD '" + strTable2
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    }
    if (strTable3 != null) {
      query3 = "records3 = LOAD '" + strTable3
          + "' USING org.apache.hadoop.zebra.pig.TableLoader();";
    }

    int count1 = 0;
    int count2 = 0;
    int count3 = 0;

    if (query1 != null) {
      System.out.println(query1);
      pigServer.registerQuery(query1);
      Iterator<Tuple> it = pigServer.openIterator("records1");
      while (it.hasNext()) {
        count1++;
        Tuple RowValue = it.next();
        System.out.println(RowValue);
        // test 1 us table
        if (query1.contains("test1") || query1.contains("test2")) {
          if (count1 == 1) {
            Assert.assertEquals("nouse", RowValue.get(0));
            Assert.assertEquals(5, RowValue.get(1));
          }
          if (count1 == 2) {
            Assert.assertEquals("nowhere", RowValue.get(0));
            Assert.assertEquals(4, RowValue.get(1));
          }
          if (count1 == 3) {
            Assert.assertEquals("us", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
          if (count1 == 4) {
            Assert.assertEquals("us", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
        } // test1, test2
        if (query1.contains("test3")) {
          if (count1 == 1) {
            System.out.println("test3: rowvalue: " + RowValue.toString());
            Assert.assertEquals("us", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
          if (count1 == 2) {
            Assert.assertEquals("us", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
          if (count1 == 3) {
            Assert.assertEquals("nowhere", RowValue.get(0));
            Assert.assertEquals(4, RowValue.get(1));
          }
          if (count1 == 4) {
            Assert.assertEquals("nouse", RowValue.get(0));
            Assert.assertEquals(5, RowValue.get(1));
          }
        } // test3
      }// while
      if (query1.contains("test1") || query1.contains("test2")
          || query1.contains("test3")) {
        Assert.assertEquals(4, count1);
      }
    }// if query1 != null

    if (query2 != null) {
      pigServer.registerQuery(query2);
      Iterator<Tuple> it = pigServer.openIterator("records2");

      while (it.hasNext()) {
        count2++;
        Tuple RowValue = it.next();
        System.out.println(RowValue);

        // if test1 india table
        if (query2.contains("test1")) {
          if (count2 == 1) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(3, RowValue.get(1));
          }
          if (count2 == 2) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(4, RowValue.get(1));
          }
        }// if test1
        if (query1.contains("test2")) {
          if (count2 == 1) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(4, RowValue.get(1));
          }
          if (count2 == 2) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(3, RowValue.get(1));
          }
        }// if test2

        if (query1.contains("test3")) {
          if (count1 == 1) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(3, RowValue.get(1));
          }
          if (count1 == 2) {
            Assert.assertEquals("india", RowValue.get(0));
            Assert.assertEquals(4, RowValue.get(1));
          }
        } // test3

      }// while
      if (query2.contains("test1") || query2.contains("test2")
          || query2.contains("test3")) {
        Assert.assertEquals(2, count2);
      }
    }// if query2 != null
    // test 1 us table should have 4 lines

    if (query3 != null) {
      pigServer.registerQuery(query3);
      Iterator<Tuple> it = pigServer.openIterator("records3");

      while (it.hasNext()) {
        count3++;
        Tuple RowValue = it.next();
        System.out.println(RowValue);

        // if test1 japan table
        if (query3.contains("test1")) {
          if (count3 == 1) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(1, RowValue.get(1));
          }
          if (count3 == 2) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
        }// if test1 test2 japan table

        if (query3.contains("test2")) {
          if (count3 == 1) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
          if (count3 == 2) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(1, RowValue.get(1));
          }
        }// test2
        if (query1.contains("test3")) {
          if (count1 == 1) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(1, RowValue.get(1));
          }
          if (count1 == 2) {
            Assert.assertEquals("japan", RowValue.get(0));
            Assert.assertEquals(2, RowValue.get(1));
          }
        } // test3

      }// while
      if (query3.contains("test1") || query3.contains("test2")
          || query3.contains("test3")) {
        Assert.assertEquals(2, count3);
      }
    }// if query2 != null
  }

  @Test
  public void test1() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    /*
     * test combine sort keys
     */
    System.out.println("******Start  testcase: " + getCurrentMethodName());
    sortKey = "word,count";
    System.out.println("hello sort on word and count");
    String methodName = getCurrentMethodName();
    String myMultiLocs = null;
    List<Path> paths = new ArrayList<Path>(3);
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      myMultiLocs = new String("/user/" + System.getenv("USER") + "/" + "us"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "india"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "japan"
          + methodName);
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "us" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "india" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "japan" + methodName)));
    } else {
      RawLocalFileSystem rawLFS = new RawLocalFileSystem();
      fs = new LocalFileSystem(rawLFS);
      myMultiLocs = new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "india"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "japan"
          + methodName);
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "india"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "japan"
          + methodName)));

    }
    getTablePaths(myMultiLocs);
    System.out.println("strTable1: " + strTable1.toString());
    System.out.println("strTable2: " + strTable2.toString());
    removeDir(new Path(strTable1));
    removeDir(new Path(strTable2));
    removeDir(new Path(strTable3));
    runMR(sortKey, paths.toArray(new Path[3]));
    checkTable(myMultiLocs);
    System.out.println("DONE test 1");

  }

  @Test
  public void test2() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    /*
     * test 'word' sort key
     */
    System.out.println("******Start  testcase: " + getCurrentMethodName());
    sortKey = "word";
    System.out.println("hello sort on word and count");
    String methodName = getCurrentMethodName();
    String myMultiLocs = null;
    List<Path> paths = new ArrayList<Path>(3);
    System.out.println("which cluster: " + whichCluster);
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      myMultiLocs = new String("/user/" + System.getenv("USER") + "/" + "us"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "india"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "japan"
          + methodName);

      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "us" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "india" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "japan" + methodName)));
    } else {
      RawLocalFileSystem rawLFS = new RawLocalFileSystem();
      fs = new LocalFileSystem(rawLFS);
      myMultiLocs = new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "india"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "japan"
          + methodName);
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "india"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "japan"
          + methodName)));
    }
    getTablePaths(myMultiLocs);
    removeDir(new Path(strTable1));
    removeDir(new Path(strTable2));
    removeDir(new Path(strTable3));
    runMR(sortKey, paths.toArray(new Path[3]));
    checkTable(myMultiLocs);
  }

  @Test
  public void test3() throws ParseException, IOException,
      org.apache.hadoop.zebra.parser.ParseException, Exception {
    /*
     * test 'count' sort key
     */
    // setUpOnce();
    System.out.println("******Start  testcase: " + getCurrentMethodName());
    sortKey = "count";
    System.out.println("hello sort on word and count, which cluster: "
        + whichCluster);
    String methodName = getCurrentMethodName();
    String myMultiLocs = null;
    List<Path> paths = new ArrayList<Path>(3);
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      myMultiLocs = new String("/user/" + System.getenv("USER") + "/" + "us"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "india"
          + methodName + "," + "/user/" + System.getenv("USER") + "/" + "japan"
          + methodName);

      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "us" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "india" + methodName)));
      paths.add(new Path(new String("/user/" + System.getenv("USER") + "/"
          + "japan" + methodName)));
    } else {

      RawLocalFileSystem rawLFS = new RawLocalFileSystem();
      fs = new LocalFileSystem(rawLFS);
      myMultiLocs = new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "india"
          + methodName + "," + fs.getWorkingDirectory() + "/" + "japan"
          + methodName);
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "us"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "india"
          + methodName)));
      paths.add(new Path(new String(fs.getWorkingDirectory() + "/" + "japan"
          + methodName)));
    }
    getTablePaths(myMultiLocs);
    removeDir(new Path(strTable1));
    removeDir(new Path(strTable2));
    removeDir(new Path(strTable3));
    runMR(sortKey, paths.toArray(new Path[3]));
    checkTable(myMultiLocs);
  }

  static class MapClass implements
      Mapper<LongWritable, Text, BytesWritable, Tuple> {
    private BytesWritable bytesKey;
    private Tuple tupleRow;
    private Object javaObj;

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
        throws IOException {
      // value should contain "word count"
      String[] wdct = value.toString().split(" ");
      if (wdct.length != 2) {
        // LOG the error
        return;
      }

      byte[] word = wdct[0].getBytes();
      bytesKey.set(word, 0, word.length);
      System.out.println("word: " + new String(word));
      tupleRow.set(0, new String(word));
      tupleRow.set(1, Integer.parseInt(wdct[1]));
      System.out.println("count:  " + Integer.parseInt(wdct[1]));

      // This key has to be created by user
      /*
       * Tuple userKey = new DefaultTuple(); userKey.append(new String(word));
       * userKey.append(Integer.parseInt(wdct[1]));
       */
      System.out.println("in map, sortkey: " + sortKey);
      Tuple userKey = new DefaultTuple();
      if (sortKey.equalsIgnoreCase("word,count")) {
        userKey.append(new String(word));
        userKey.append(Integer.parseInt(wdct[1]));
      }

      if (sortKey.equalsIgnoreCase("count")) {
        userKey.append(Integer.parseInt(wdct[1]));
      }

      if (sortKey.equalsIgnoreCase("word")) {
        userKey.append(new String(word));
      }

      try {

        /* New M/R Interface */
        /* Converts user key to zebra BytesWritable key */
        /* using sort key expr tree */
        /* Returns a java base object */
        /* Done for each user key */

        bytesKey = BasicTableOutputFormat.getSortKey(javaObj, userKey);
      } catch (Exception e) {

      }

      output.collect(bytesKey, tupleRow);
    }

    @Override
    public void configure(JobConf job) {
      bytesKey = new BytesWritable();
      sortKey = job.get("sortKey");
      try {
        Schema outSchema = BasicTableOutputFormat.getSchema(job);
        tupleRow = TypesUtils.createTuple(outSchema);
        javaObj = BasicTableOutputFormat.getSortKeyGenerator(job);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (org.apache.hadoop.zebra.parser.ParseException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  static class ReduceClass implements
      Reducer<BytesWritable, Tuple, BytesWritable, Tuple> {
    Tuple outRow;

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }

    public void reduce(BytesWritable key, Iterator<Tuple> values,
        OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
        throws IOException {
      try {
        for (; values.hasNext();) {
          output.collect(key, values.next());
        }
      } catch (ExecException e) {
        e.printStackTrace();
      }
    }

  }

  static class OutputPartitionerClass extends ZebraOutputPartition {

    @Override
    public int getOutputPartition(BytesWritable key, Tuple value) {

      // System.out.println(this.jobConf);

      String reg = null;
      try {
        reg = (String) (value.get(0));
      } catch (Exception e) {
        //
      }

      if (reg.equals("us"))
        return 0;
      if (reg.equals("india"))
        return 1;
      if (reg.equals("japan"))
        return 2;

      return 0;
    }

  }

  public void runMR(String sortKey, Path... paths) throws ParseException,
      IOException, Exception, org.apache.hadoop.zebra.parser.ParseException {

    JobConf jobConf = new JobConf(conf);
    //XXX
    jobConf.setJobName("TestMultipleOutputsTypeApi");
    jobConf.setJarByClass(TestMultipleOutputsTypeApi.class);
    jobConf.set("table.output.tfile.compression", "gz");
    jobConf.set("sortKey", sortKey);
    // input settings
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(TestMultipleOutputsTypeApi.MapClass.class);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(ZebraTuple.class);
    FileInputFormat.setInputPaths(jobConf, inputPath);

    jobConf.setNumMapTasks(1);

    // output settings

    jobConf.setOutputFormat(BasicTableOutputFormat.class);

    String schema = "word:string, count:int";
    String storageHint = "[word];[count]";
    BasicTableOutputFormat.setMultipleOutputs(jobConf,
        TestMultipleOutputsTypeApi.OutputPartitionerClass.class, paths);
    ZebraSchema zSchema = ZebraSchema.createZebraSchema(schema);
    ZebraStorageHint zStorageHint = ZebraStorageHint
        .createZebraStorageHint(storageHint);
    ZebraSortInfo zSortInfo = ZebraSortInfo.createZebraSortInfo(sortKey, null);
    BasicTableOutputFormat.setStorageInfo(jobConf, zSchema, zStorageHint,
        zSortInfo);

    jobConf.setNumReduceTasks(1);
    JobClient.runJob(jobConf);
    BasicTableOutputFormat.close(jobConf);
  }

  @Override
  public int run(String[] args) throws Exception {
    TestMultipleOutputsTypeApi test = new TestMultipleOutputsTypeApi();
    TestMultipleOutputsTypeApi.setUpOnce();
    System.out.println("after setup");
    
    test.test1();
    test.test2();
    test.test3();
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    //XXX
    System.out.println("*******************  this is new today");

    conf = new Configuration();
    
    int res = ToolRunner.run(conf, new TestMultipleOutputsTypeApi(), args);
    
    System.exit(res);
  }
}
