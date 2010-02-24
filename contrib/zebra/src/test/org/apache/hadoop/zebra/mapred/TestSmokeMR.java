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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * This is a sample a complete MR sample code for Table. It doens't contain
 * 'read' part. But, it should be similar and easier to write. Refer to test
 * cases in the same directory.
 * 
 * Assume the input files contain rows of word and count, separated by a space:
 * 
 * <pre>
 * this 2
 * is 1
 * a 4 
 * test 2 
 * hello 1 
 * world 3
 * </pre>
 * 
 */
public class TestSmokeMR extends Configured implements Tool{
  static String inputPath;
  static String outputPath;
  static String inputFileName = "smoke.txt";
  static String outputTableName ="smokeTable";
  protected static ExecType execType = ExecType.MAPREDUCE;
  private static MiniCluster cluster;
  protected static PigServer pigServer;
  private static Configuration conf;
  public static String sortKey = null;

  private static FileSystem fs;

  private static String zebraJar;
  private static String whichCluster;
 
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
        tupleRow.set(0, new String(word));
        tupleRow.set(1, Integer.parseInt(wdct[1]));

        // This key has to be created by user
        Tuple userKey = new ZebraTuple();
        userKey.append(new String(word));
        userKey.append(Integer.parseInt(wdct[1]));
        try {

            /* New M/R Interface */
            /* Converts user key to zebra BytesWritable key */
            /* using sort key expr tree  */
            /* Returns a java base object */
            /* Done for each user key */
        	
          bytesKey = BasicTableOutputFormat.getSortKey(javaObj, userKey);
        } catch(Exception e) {
        	
        }

        output.collect(bytesKey, tupleRow);
    }

    @Override
    public void configure(JobConf job) {
      bytesKey = new BytesWritable();
      try {
        Schema outSchema = BasicTableOutputFormat.getSchema(job);
        tupleRow = TypesUtils.createTuple(outSchema);
        
        /* New M/R Interface */
        /* returns an expression tree for sort keys */
        /* Returns a java base object */
        /* Done once per table */
        javaObj = BasicTableOutputFormat.getSortKeyGenerator(job);
        
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ParseException e) {
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
    		for(; values.hasNext();)  {
    	      output.collect(key, values.next());
    		}  
    	  } catch (ExecException e) {
    	    e.printStackTrace();
    	  }
    }
  
  }  
  
  @BeforeClass
  public static void setUpOnce() throws IOException {
    if (System.getenv("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System
          .setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
    }

    if (System.getProperty("whichCluster") == null) {
      System.setProperty("whichCluster", "realCluster");
      System.out.println("should be called");
      whichCluster = System.getProperty("whichCluster");
    } else {
      whichCluster = System.getProperty("whichCluster");
    }

    System.out.println("clusterddddd: " + whichCluster);
    System.out.println(" get env hadoop home: " + System.getenv("HADOOP_HOME"));
    System.out.println(" get env user name: " + System.getenv("USER"));
    if ((whichCluster.equalsIgnoreCase("realCluster") && System
        .getenv("HADOOP_HOME") == null)) {
      System.out.println("Please set HADOOP_HOME");
      System.exit(0);
    }

    if ( conf == null ) {
      conf = new Configuration();
    }

    if ((whichCluster.equalsIgnoreCase("realCluster") && System.getenv("USER") == null)) {
      System.out.println("Please set USER");
      System.exit(0);
    }
//    zebraJar = System.getenv("HADOOP_HOME") + "/lib/zebra.jar";
    zebraJar = System.getenv("ZEBRA_JAR");
    File file = new File(zebraJar);
    if (!file.exists() && whichCluster.equalsIgnoreCase("realCluster")) {
      System.out.println("Please put zebra.jar at hadoop_home/lib");
   //   System.exit(0);
    }

    // set inputPath and outPath
    String workingDir = null;
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      inputPath = new String("/user/" + System.getenv("USER") + "/"
          + inputFileName);
      System.out.println("inputPath: " + inputPath);
      outputPath =  new String("/user/" + System.getenv("USER") + "/" +outputTableName);
      fs = new Path(inputPath).getFileSystem(conf);

    } else {
      RawLocalFileSystem rawLFS = new RawLocalFileSystem();
      fs = new LocalFileSystem(rawLFS);
      workingDir = fs.getWorkingDirectory().toString().split(":")[1];
      inputPath = new String(workingDir + "/" + inputFileName);
      outputPath = new String(workingDir + "/" + outputTableName);
      System.out.println("inputPath: " + inputPath);
 
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
  public static void checkTableExists(boolean expected, String strDir)
      throws IOException {

    File theDir = null;
    boolean actual = false;
    if (whichCluster.equalsIgnoreCase("miniCluster")) {
      theDir = new File(strDir.split(":")[1]);
      actual = theDir.exists();

    }
    if (whichCluster.equalsIgnoreCase("realCluster")) {
      theDir = new File(strDir.split(":")[0]);
      actual = fs.exists(new Path(theDir.toString()));
    }
    System.out.println("the dir : " + theDir.toString());

    if (actual != expected) {
      Assert.fail("dir exists or not is different from what expected.");
    }
  }
  public static void removeDir(Path outPath) throws IOException {
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
  public static void main(String[] args) throws ParseException, IOException, Exception {
    int res = ToolRunner.run(new Configuration(), new TestSmokeMR(), args);
    System.out.println("res: "+res);
    checkTableExists(true, outputPath);
    if (res == 0) {
      System.out.println("TEST PASSED!");
    } else { 
       System.out.println("TEST FAILED");
       throw new IOException("Zebra MR Smoke Test Failed");
    }   
    
  }

  @Override
  public int run(String[] arg0) throws Exception {
    conf = getConf();
    TestSmokeMR.setUpOnce();
    removeDir(new Path(outputPath));
    JobConf jobConf = new JobConf(conf,TestSmokeMR.class);

    jobConf.setJobName("TableMRSortedTableZebraKeyGenerator");
    jobConf.set("table.output.tfile.compression", "gz");
    jobConf.setJarByClass(TestSmokeMR.class);
    // input settings
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(TestSmokeMR.MapClass.class);
    jobConf.setReducerClass(TestSmokeMR.ReduceClass.class);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(ZebraTuple.class);
    FileInputFormat.setInputPaths(jobConf, inputPath);
    jobConf.setNumMapTasks(1);

    // output settings
   jobConf.setOutputFormat(BasicTableOutputFormat.class);
    BasicTableOutputFormat.setOutputPath(jobConf, new Path(outputPath));
    // set the logical schema with 2 columns
    BasicTableOutputFormat.setSchema(jobConf, "word:string, count:int");
    // for demo purposes, create 2 physical column groups
    BasicTableOutputFormat.setStorageHint(jobConf, "[word];[count]");
    
    /* New M/R Interface */
    /* Set sort columns in a comma separated string */
    /* Each sort column should belong to schema columns */
    BasicTableOutputFormat.setSortInfo(jobConf, "word, count");

    // set map-only job.
    jobConf.setNumReduceTasks(1);
    JobClient.runJob(jobConf);
    BasicTableOutputFormat.close(jobConf);
    return 0;
  }
}
