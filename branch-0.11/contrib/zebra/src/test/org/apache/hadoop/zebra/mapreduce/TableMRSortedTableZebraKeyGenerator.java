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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

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
public class TableMRSortedTableZebraKeyGenerator {
	static class MapClass extends
	Mapper<LongWritable, Text, BytesWritable, Tuple> {
		private BytesWritable bytesKey;
		private Tuple tupleRow;
		private Object javaObj;

		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
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
			Tuple userKey = new DefaultTuple();
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

			context.write(bytesKey, tupleRow);
		}

		@Override
		public void setup(Context context) {
			bytesKey = new BytesWritable();
			try {
				Schema outSchema = BasicTableOutputFormat.getSchema(context);
				tupleRow = TypesUtils.createTuple(outSchema);

				/* New M/R Interface */
				/* returns an expression tree for sort keys */
				/* Returns a java base object */
				/* Done once per table */
				javaObj = BasicTableOutputFormat.getSortKeyGenerator(context);

			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}

	}

	static class ReduceClass extends
	Reducer<BytesWritable, Tuple, BytesWritable, Tuple> {
		Tuple outRow;

		public void reduce(BytesWritable key, Iterator<Tuple> values, Context context)
		throws IOException, InterruptedException {
			try {
				for(; values.hasNext();)  {
					context.write(key, values.next());
				}  
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}

	}  

	public static void main(String[] args) throws ParseException, IOException, 
	InterruptedException, ClassNotFoundException {
		Job job = new Job();
		job.setJobName("tableMRSample");
		Configuration conf = job.getConfiguration();
		conf.set("table.output.tfile.compression", "gz");

		// input settings
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TableMRSortedTableZebraKeyGenerator.MapClass.class);
		job.setReducerClass(TableMRSortedTableZebraKeyGenerator.ReduceClass.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(DefaultTuple.class);
		FileInputFormat.setInputPaths(job, new Path(
				"/home/gauravj/work/grid/myTesting/input.txt"));

		// TODO: need to find a replacement.
		//job.setNumMapTasks(1);

		// output settings
		Path outPath = new Path("/home/gauravj/work/grid/myTesting/tableOuts");
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		BasicTableOutputFormat.setOutputPath(job, outPath);
		// set the logical schema with 2 columns
		BasicTableOutputFormat.setSchema(job, "word:string, count:int");
		// for demo purposes, create 2 physical column groups
		BasicTableOutputFormat.setStorageHint(job, "[word];[count]");

		/* New M/R Interface */
		/* Set sort columns in a comma separated string */
		/* Each sort column should belong to schema columns */
		BasicTableOutputFormat.setSortInfo(job, "word, count");

		// set map-only job.
		job.setNumReduceTasks(1);
		job.submit();
	}
}
