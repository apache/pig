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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POStreamSpark;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.pig.data.Tuple;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamConverter implements
		POConverter<Tuple, Tuple, POStreamSpark> {
	private static Log LOG = LogFactory.getLog(StreamConverter.class);
	private byte[] confBytes;

	public StreamConverter(byte[] confBytes) {
		this.confBytes = confBytes;
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			POStreamSpark poStream) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, poStream, 1);
		RDD<Tuple> rdd = predecessors.get(0);
		RDD<Tuple> rdd2 = rdd.coalesce(1, false, null);
		long count = 0;
		try {
			count = rdd2.count();
		} catch (Exception e) {
			System.out.println("Crash in StreamConverter :" + e);
			LOG.info("Crash in StreamConverter ", e);
		}
		StreamFunction streamFunction = new StreamFunction(poStream, count,
				confBytes);
		return rdd2.toJavaRDD().mapPartitions(streamFunction, true).rdd();
	}

	private static class StreamFunction implements
			FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {
		private POStreamSpark poStream;
		private long total_limit;
		private long current_val;
		private boolean proceed = false;
		private transient JobConf jobConf;
		private byte[] confBytes;

		private StreamFunction(POStreamSpark poStream, long total_limit,
				byte[] confBytes) {
			this.poStream = poStream;
			this.total_limit = total_limit;
			this.current_val = 0;
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
					return new POOutputConsumerIterator(input) {

						@Override
						protected void attach(Tuple tuple) {
							poStream.setInputs(null);
							poStream.attachInput(tuple);
							try {
								current_val = current_val + 1;
								if (current_val == total_limit) {
									proceed = true;
								} else {
									proceed = false;
								}

							} catch (Exception e) {
								System.out.println("Crash in StreamConverter :"
										+ e);
								LOG.info("Crash in StreamConverter ", e);
							}
						}

						@Override
						protected Result getNextResult() throws ExecException {
							Result result = poStream.getNextTuple(proceed);
							return result;
						}
					};
				}
			};
		}
	}
}
