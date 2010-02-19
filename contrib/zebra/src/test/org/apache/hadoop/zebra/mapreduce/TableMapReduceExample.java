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

package org.apache.hadoop.zebra.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
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

	static class Map extends Mapper<LongWritable, Text, BytesWritable, Tuple> {
		private BytesWritable bytesKey;
		private Tuple tupleRow;

		/**
		 * Map method for reading input.
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

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

			context.write( bytesKey, tupleRow );
		}

		/**
		 * Configuration of the job. Here we create an empty Tuple Row.
		 */
		@Override
		public void setup(Context context) {
			bytesKey = new BytesWritable();
			try {
				Schema outSchema = BasicTableOutputFormat.getSchema( context );
				tupleRow = TypesUtils.createTuple(outSchema);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}

	}

	static class ProjectionMap extends
	Mapper<BytesWritable, Tuple, Text, IntWritable> {
		private final static Text all = new Text("All");

		/**
		 * Map method which gets count column after projection.
		 * 
		 * @throws IOException
		 */
		@Override
		public void map(BytesWritable key, Tuple value, Context context)
		throws IOException, InterruptedException {
			context.write( all, new IntWritable((Integer) value.get(0)) );
		}
	}

	public static class ProjectionReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * Reduce method which implements summation. Acts as both reducer and
		 * combiner.
		 * 
		 * @throws IOException
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Where jobs and their settings and sequence is set.
	 * 
	 * @param args
	 *          arguments with exception of Tools understandable ones.
	 */
	public int run(String[] args) throws Exception {
		if (args == null || args.length != 3) {
			System.out
			.println("usage: TableMapReduceExample input_path_for_text_file output_path_for_table output_path_for_text_file");
			System.exit(-1);
		}

		/*
		 * First MR Job creating a Table with two columns
		 */
		Job job = new Job();
		job.setJobName("TableMapReduceExample");
		Configuration conf = job.getConfiguration();
		conf.set("table.output.tfile.compression", "none");

		// Input settings
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Map.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// Output settings
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		BasicTableOutputFormat.setOutputPath( job, new Path(args[1]) );

		// set the logical schema with 2 columns
		BasicTableOutputFormat.setSchema( job, "word:string, count:int" );

		// for demo purposes, create 2 physical column groups
		BasicTableOutputFormat.setStorageHint( job, "[word];[count]" );

		// set map-only job.
		job.setNumReduceTasks(0);

		// Run Job
		job.submit();

		/*
		 * Second MR Job for Table Projection of count column
		 */
		Job projectionJob = new Job();
		projectionJob.setJobName("TableProjectionMapReduceExample");
		conf = projectionJob.getConfiguration();

		// Input settings
		projectionJob.setMapperClass(ProjectionMap.class);
		projectionJob.setInputFormatClass(TableInputFormat.class);
		TableInputFormat.setProjection(job, "count");
		TableInputFormat.setInputPaths(job, new Path(args[1]));
		projectionJob.setMapOutputKeyClass(Text.class);
		projectionJob.setMapOutputValueClass(IntWritable.class);

		// Output settings
		projectionJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(projectionJob, new Path(args[2]));
		projectionJob.setReducerClass(ProjectionReduce.class);
		projectionJob.setCombinerClass(ProjectionReduce.class);

		// Run Job
		projectionJob.submit();

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TableMapReduceExample(),
				args);
		System.exit(res);
	}
}
