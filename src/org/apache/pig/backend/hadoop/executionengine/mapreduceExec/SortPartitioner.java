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
package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;


public class SortPartitioner implements Partitioner {
	Tuple[] quantiles;
	WritableComparator comparator;
	
	public int getPartition(WritableComparable key, Writable value,
			int numPartitions) {
		try{
			Tuple keyTuple = (Tuple)key;
			int index = Arrays.binarySearch(quantiles, keyTuple.getTupleField(0), comparator);
			if (index < 0)
				index = -index-1;
			return Math.min(index, numPartitions - 1);
		}catch(IOException e){
			throw new RuntimeException(e);
		}
	}

	public void configure(JobConf job) {
		String quantilesFile = job.get("pig.quantilesFile", "");
		if (quantilesFile.length() == 0)
			throw new RuntimeException("Sort paritioner used but no quantiles found");
		
		try{
			InputStream is = FileLocalizer.openDFSFile(quantilesFile,job);
			BinStorage loader = new BinStorage();
			loader.bindTo(quantilesFile, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
			
			Tuple t;
			ArrayList<Tuple> quantiles = new ArrayList<Tuple>();
			
			while(true){
				t = loader.getNext();
				if (t==null)
					break;
				quantiles.add(t);
			}
			this.quantiles = quantiles.toArray(new Tuple[0]);
		}catch (IOException e){
			throw new RuntimeException(e);
		}

		comparator = job.getOutputKeyComparator();
	}

}
