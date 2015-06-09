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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.FlatMapFunction;

public class StreamConverter implements
		RDDConverter<Tuple, Tuple, POStream> {
	private byte[] confBytes;

	public StreamConverter(byte[] confBytes) {
		this.confBytes = confBytes;
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			POStream poStream) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, poStream, 1);
		RDD<Tuple> rdd = predecessors.get(0);
		StreamFunction streamFunction = new StreamFunction(poStream, confBytes);
		return rdd.toJavaRDD().mapPartitions(streamFunction, true).rdd();
	}

	private static class StreamFunction implements
			FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {
		private POStream poStream;
		private transient JobConf jobConf;
		private byte[] confBytes;

		private StreamFunction(POStream poStream, byte[] confBytes) {
			this.poStream = poStream;
			this.confBytes = confBytes;
		}

		void initializeJobConf() {
			if (this.jobConf == null) {
				this.jobConf = KryoSerializer
						.deserializeJobConf(this.confBytes);
				PigMapReduce.sJobConfInternal.set(jobConf);
				try {
					MapRedUtil.setupUDFContext(jobConf);
					PigContext pc = (PigContext) ObjectSerializer
							.deserialize(jobConf.get("pig.pigContext"));
					SchemaTupleBackend.initialize(jobConf, pc);

				} catch (IOException ioe) {
					String msg = "Problem while configuring UDFContext from ForEachConverter.";
					throw new RuntimeException(msg, ioe);
				}
			}
		}

		public Iterable<Tuple> call(final Iterator<Tuple> input) {
			initializeJobConf();
			return new Iterable<Tuple>() {
				@Override
				public Iterator<Tuple> iterator() {
					return new OutputConsumerIterator(input) {

						@Override
						protected void attach(Tuple tuple) {
							poStream.setInputs(null);
							poStream.attachInput(tuple);
						}

						@Override
						protected Result getNextResult() throws ExecException {
							Result result = poStream.getNextTuple();
							return result;
						}

						@Override
						protected void endOfInput() {
							poStream.setFetchable(true);
						}
					};
				}
			};
		}
	}
}
