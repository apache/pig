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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.zebra.mapred.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Iterator;

/**
 * <code>TableMapReduceExample<code> is a map-reduce example for Table Input/Output Format.
 * <p/>
 * Schema for Table is set to two columns containing Word of type <i>string</i> and Count of type <i>int</i> using <code> BasicTableOutputFormat.setSchema(jobConf, "word:string, count:int"); </code>
 * <p/>
 * Hint for creation of Column Groups is specified using
 * <code>  BasicTableOutputFormat.setStorageHint(jobConf, "[word];[count]"); </code>
 * . Here we have two column groups.
 * <p/>
 * Input file should contain rows of word and count, separated by a space. For
 * example:
 * 
 * <pre>
 * this 2
 * is 1
 * a 5
 * test 2
 * hello 1
 * world 3
 * </pre>
 * <p/>
 * <p>
 * Second job reads output from the first job which is in Table Format. Here we
 * specify <i>count</i> as projection column. Table Input Format projects in put
 * row which has both word and count into a row containing only the count column
 * and hands it to map.
 * <p/>
 * Reducer sums the counts and produces a sum of counts which should match total
 * number of words in original text.
 */

public class TableMapReduceExample extends Configured implements Tool {
  private static Configuration conf = null;

  static class MyMap extends MapReduceBase implements
      Mapper<LongWritable, Text, BytesWritable, Tuple> {
    private BytesWritable bytesKey;
    private Tuple tupleRow;

    /**
     * Map method for reading input.
     */
    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
        throws IOException {

      // value should contain "word count"
      String[] wordCount = value.toString().split(" ");
      if (wordCount.length != 2) {
        // LOG the error
        throw new IOException("Value does not contain two fields:" + value);
      }

      byte[] word = wordCount[0].getBytes();
      bytesKey.set(word, 0, word.length);
      tupleRow.set(0, new String(word));
      tupleRow.set(1, Integer.parseInt(wordCount[1]));

      output.collect(bytesKey, tupleRow);
    }

    /**
     * Configuration of the job. Here we create an empty Tuple Row.
     */
    @Override
    public void configure(JobConf job) {
      bytesKey = new BytesWritable();
      try {
        Schema outSchema = BasicTableOutputFormat.getSchema(job);
        tupleRow = TypesUtils.createTuple(outSchema);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }

  }

  static class ProjectionMap extends MapReduceBase implements
      Mapper<BytesWritable, Tuple, Text, IntWritable> {
    private final static Text all = new Text("All");

    /**
     * Map method which gets count column after projection.
     * 
     * @throws IOException
     */
    @Override
    public void map(BytesWritable key, Tuple value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      output.collect(all, new IntWritable((Integer) value.get(0)));
    }
  }

  public static class ProjectionReduce extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * Reduce method which implements summation. Acts as both reducer and
     * combiner.
     * 
     * @throws IOException
     */
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  /**
   * Where jobs and their settings and sequence is set.
   * 
   * @param args
   *          arguments with exception of Tools understandable ones.
   */
  @Override
  public int run(String[] args) throws Exception {
    if (args == null || args.length != 3) {
      System.out
          .println("usage: TableMapReduceExample input_path_for_text_file output_path_for_table output_path_for_text_file");
      System.exit(-1);
    }

    /*
     * First MR Job creating a Table with two columns
     */
    //Configuration conf = getConf();
    JobConf jobConf = new JobConf(conf);
    jobConf.setJarByClass(TableMapReduceExample.class);

    jobConf.setJobName("TableMapReduceExample");
    jobConf.set("table.output.tfile.compression", "none");

    // Input settings
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(MyMap.class);
    FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
    //FileInputFormat.setInputPaths(jobConf, new Path("/tmp/input.txt"));
    
    // Output settings
    jobConf.setOutputFormat(BasicTableOutputFormat.class);
    BasicTableOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    //BasicTableOutputFormat.setOutputPath(jobConf, new Path("/tmp/t1"));

    // set the logical schema with 2 columns
    BasicTableOutputFormat.setSchema(jobConf, "word:string, count:int");

    // for demo purposes, create 2 physical column groups
    BasicTableOutputFormat.setStorageHint(jobConf, "[word];[count]");

    // set map-only job.
    jobConf.setNumReduceTasks(0);

    // Run Job
    JobClient.runJob(jobConf);
    

    /*
     * Second MR Job for Table Projection of count column
     */
    //JobConf projectionJobConf = new JobConf();
    JobConf projectionJobConf = new JobConf(conf);
    projectionJobConf.setJobName("TableProjectionMapReduceExample");

    // Input settings
    projectionJobConf.setJarByClass(TableMapReduceExample.class);
    projectionJobConf.setMapperClass(ProjectionMap.class);
    projectionJobConf.setInputFormat(TableInputFormat.class);
    TableInputFormat.setProjection(projectionJobConf, "count");
    TableInputFormat.setInputPaths(projectionJobConf, new Path(args[1]));
    projectionJobConf.setMapOutputKeyClass(Text.class);
    projectionJobConf.setMapOutputValueClass(IntWritable.class);

    // Output settings
    projectionJobConf.setOutputFormat(TextOutputFormat.class);
    Path p2 = new Path(args[2]);
    FileSystem fs = p2.getFileSystem(conf);
    if (fs.exists(p2)) {
      fs.delete(p2, true);
    }
    FileOutputFormat.setOutputPath(projectionJobConf, p2);
    
    projectionJobConf.setReducerClass(ProjectionReduce.class);
    projectionJobConf.setCombinerClass(ProjectionReduce.class);

    // Run Job
    JobClient.runJob(projectionJobConf);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("*******************  this is new now");

    conf = new Configuration();
    
    //int res = ToolRunner.run(new Configuration(), new TableMapReduceExample(), args);
    int res = ToolRunner.run(conf, new TableMapReduceExample(), args);
    
    System.exit(res);
  }
}
