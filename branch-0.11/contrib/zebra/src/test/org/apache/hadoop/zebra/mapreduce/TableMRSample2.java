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
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
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
	static class MapClass extends
	Mapper<BytesWritable, Tuple, BytesWritable, Tuple> {
		private BytesWritable bytesKey;
		private Tuple tupleRow;

		@Override
		public void map(BytesWritable key, Tuple value, Context context)
		throws IOException, InterruptedException {
			System.out.println(key.toString() + value.toString());
			context.write(key, value);
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

		public static void main(String[] args) throws ParseException, IOException, 
		InterruptedException, ClassNotFoundException {
			Job job = new Job();
			job.setJobName("tableMRSample");
			Configuration conf = job.getConfiguration();
			conf.set("table.output.tfile.compression", "gz");

			// input settings
			job.setInputFormatClass(TableInputFormat.class);
			job.setOutputFormatClass(BasicTableOutputFormat.class);
			job.setMapperClass(TableMRSample2.MapClass.class);

			List<Path> paths = new ArrayList<Path>(2);
			Path p = new Path("/homes/chaow/mapredu/t1");
			System.out.println("path = " + p);
			paths.add(p);
			p = new Path("/homes/chaow/mapredu/t2");
			paths.add(p);

			TableInputFormat.setInputPaths(job, paths.toArray(new Path[2]));
			TableInputFormat.setProjection(job, "word");
			BasicTableOutputFormat.setOutputPath(job, new Path(
			"/homes/chaow/mapredu2/t1"));

			BasicTableOutputFormat.setSchema(job, "word:string");
			BasicTableOutputFormat.setStorageHint(job, "[word]");

			// set map-only job.
			job.setNumReduceTasks(0);
			// TODO: need to find a replacement
			//job.setNumMapTasks(2);
			job.submit();
		}
	}
}
