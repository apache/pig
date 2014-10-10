package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;


@SuppressWarnings({ "serial"})
public class CollectedGroupConverter implements POConverter<Tuple, Tuple, POCollectedGroup> {

	@Override
  public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
      POCollectedGroup physicalOperator) throws IOException {
    SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
    RDD<Tuple> rdd = predecessors.get(0);
    // return predecessors.get(0);
    RDD<Tuple> rdd2 = rdd.coalesce(1, false, null);
    long count = 0;
    try {

      count = rdd2.count();

    } catch (Exception e) {

    }
    CollectedGroupFunction collectedGroupFunction
        = new CollectedGroupFunction(physicalOperator, count);
    return rdd.toJavaRDD().mapPartitions(collectedGroupFunction, true).rdd();
  }

	private static class CollectedGroupFunction implements FlatMapFunction<Iterator<Tuple>, Tuple> {

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

		public Iterable<Tuple> call(final Iterator<Tuple> input) {

		  return new Iterable<Tuple>() {

		    @Override
		    public Iterator<Tuple> iterator() {
		      return new POOutputConsumerIterator(input) {
		        protected void attach(Tuple tuple) {
		          poCollectedGroup.setInputs(null);
		          poCollectedGroup.attachInput(tuple);
		          poCollectedGroup.setParentPlan(poCollectedGroup.getPlans().get(0));

		          try{

		            current_val = current_val + 1;
		            //System.out.println("Row: =>" + current_val);
		            if (current_val == total_limit) {
		              proceed = true;
		            } else {
		              proceed = false;
		            }

		          } catch(Exception e){
		            System.out.println("Crashhh in CollectedGroupConverter :" + e);
		            e.printStackTrace();
		          }
		        }

		        protected Result getNextResult() throws ExecException {
		          return poCollectedGroup.getNextTuple(proceed);
		        }
		      };
		    }
      };
		}
	}
}