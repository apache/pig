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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.zebra.mapred.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.Tuple;

/**
 * This is a sample to show using zebra table to do a simple basic union in
 * map/reduce * To run this, we need have two basic tables ready. They contain
 * the data as in Sample 1, i.e., (word, count). In this example, they are at:
 * /homes/chaow/mapredu/t1 /homes/chaow/mapredu/t2 The resulting table is put
 * at: /homes/chaow/mapredu2/t1
 * 
 */
public class TableMRSample2 {
  static class MapClass implements
      Mapper<BytesWritable, Tuple, BytesWritable, Tuple> {
    private BytesWritable bytesKey;
    private Tuple tupleRow;

    @Override
    public void map(BytesWritable key, Tuple value,
        OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
        throws IOException

    {
      System.out.println(key.toString() + value.toString());
      output.collect(key, value);
    }

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

    @Override
    public void close() throws IOException {
      // no-op
    }

    public static void main(String[] args) throws ParseException, IOException {
      JobConf jobConf = new JobConf();
      jobConf.setJobName("tableMRSample");
      jobConf.set("table.output.tfile.compression", "gz");

      // input settings
      jobConf.setInputFormat(TableInputFormat.class);
      jobConf.setOutputFormat(BasicTableOutputFormat.class);
      jobConf.setMapperClass(TableMRSample2.MapClass.class);

      List<Path> paths = new ArrayList<Path>(2);
      Path p = new Path("/homes/chaow/mapredu/t1");
      System.out.println("path = " + p);
      paths.add(p);
      p = new Path("/homes/chaow/mapredu/t2");
      paths.add(p);

      TableInputFormat.setInputPaths(jobConf, paths.toArray(new Path[2]));
      TableInputFormat.setProjection(jobConf, "word");
      BasicTableOutputFormat.setOutputPath(jobConf, new Path(
          "/homes/chaow/mapredu2/t1"));

      BasicTableOutputFormat.setSchema(jobConf, "word:string");
      BasicTableOutputFormat.setStorageHint(jobConf, "[word]");

      // set map-only job.
      jobConf.setNumReduceTasks(0);
      jobConf.setNumMapTasks(2);
      JobClient.runJob(jobConf);
    }
  }
}
