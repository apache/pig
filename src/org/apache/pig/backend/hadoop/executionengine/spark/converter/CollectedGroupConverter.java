package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.Main;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.spark.BroadCastServer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.PlanException;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;


@SuppressWarnings({ "serial"})
public class CollectedGroupConverter implements POConverter<Tuple, Tuple, POCollectedGroup> {

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POCollectedGroup physicalOperator) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
		RDD<Tuple> rdd = predecessors.get(0);
		//return predecessors.get(0);
		RDD<Tuple> rdd2 = rdd.coalesce(1, false);
		long count = 0;
		try{

			count = rdd2.count();
			long ccount = 0;

		}catch(Exception e){

		}
		CollectedGroupFunction collectedGroupFunction = new CollectedGroupFunction(physicalOperator, count);
		return rdd.mapPartitions(collectedGroupFunction, true, SparkUtil.getManifest(Tuple.class));
	}

	private static class CollectedGroupFunction extends Function<Iterator<Tuple>, Iterator<Tuple>> implements Serializable {

		/**
		 * 
		 */
		private POCollectedGroup poCollectedGroup;

		public long total_limit;
		public long current_val;
		public boolean proceed;

		private CollectedGroupFunction(POCollectedGroup poCollectedGroup, long count) {
			this.poCollectedGroup = poCollectedGroup;
			this.total_limit = count;
			this.current_val = 0;

		}

		public Iterator<Tuple> call(Iterator<Tuple> i) {
			final java.util.Iterator<Tuple> input = JavaConversions.asJavaIterator(i);
			Iterator<Tuple> output = JavaConversions.asScalaIterator(new POOutputConsumerIterator(input) {
				protected void attach(Tuple tuple) {
					poCollectedGroup.setInputs(null);
					poCollectedGroup.attachInput(tuple);
					poCollectedGroup.setParentPlan(poCollectedGroup.getPlans().get(0));
					try{

						current_val = current_val + 1;
						//System.out.println("Row: =>" + current_val);
						if(current_val == total_limit){
							proceed = true;
						}else{
							proceed = false;
						}

					}catch(Exception e){
						System.out.println("Crashhh in CollectedGroupConverter :" + e);
						e.printStackTrace();
					}					
				}

				protected Result getNextResult() throws ExecException {
					return poCollectedGroup.getNextTuple(proceed);
				}
			});
			return output;
		}
	}

}