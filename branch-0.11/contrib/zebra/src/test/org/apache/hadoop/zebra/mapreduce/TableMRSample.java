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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
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
public class TableMRSample {
	static class MapClass extends
	Mapper<LongWritable, Text, BytesWritable, Tuple> {
		private BytesWritable bytesKey;
		private Tuple tupleRow;

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

			context.write(bytesKey, tupleRow);
		}

		@Override
		public void setup(Context context) {
			bytesKey = new BytesWritable();
			try {
				Schema outSchema = BasicTableOutputFormat.getSchema(context);
				tupleRow = TypesUtils.createTuple(outSchema);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (ParseException e) {
				throw new RuntimeException(e);
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
		job.setMapperClass(TableMRSample.MapClass.class);
		FileInputFormat.setInputPaths(job, new Path(
				"/user/joe/inputdata/input.txt"));

		// output settings
		Path outPath = new Path("/user/joe/outputdata/");
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		BasicTableOutputFormat.setOutputPath(job, outPath);
		// set the logical schema with 2 columns
		BasicTableOutputFormat.setSchema(job, "word:string, count:int");
		// for demo purposes, create 2 physical column groups
		BasicTableOutputFormat.setStorageHint(job, "[word];[count]");

		// set map-only job.
		job.setNumReduceTasks(0);
		job.submit();
	}
}
