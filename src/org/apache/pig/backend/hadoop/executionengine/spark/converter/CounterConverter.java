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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

public class CounterConverter implements RDDConverter<Tuple, Tuple, POCounter> {

    private static final Log LOG = LogFactory.getLog(CounterConverter.class);
    
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, 
            POCounter poCounter) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poCounter, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        CounterConverterFunction f = new CounterConverterFunction(poCounter);
        JavaRDD<Tuple> jRdd = rdd.toJavaRDD().mapPartitionsWithIndex(f, true);
//        jRdd = jRdd.cache();
        return jRdd.rdd();
    }
    
    @SuppressWarnings("serial")
    private static class CounterConverterFunction implements 
        Function2<Integer, Iterator<Tuple>, Iterator<Tuple>>, Serializable {

        private final POCounter poCounter;
        private long localCount = 1L;
        private long sparkCount = 0L;
        
        private CounterConverterFunction(POCounter poCounter) {
            this.poCounter = poCounter;
        }
        
        @Override
        public Iterator<Tuple> call(Integer index, final 
                Iterator<Tuple> input) {
            Tuple inp = null;
            Tuple output = null;
            long sizeBag = 0L;

            List<Tuple> listOutput = new ArrayList<Tuple>();
            
            try {
                while (input.hasNext()) {
                    inp = input.next();
                    output = TupleFactory.getInstance()
                            .newTuple(inp.getAll().size() + 3);
                    
                    for (int i = 0; i < inp.getAll().size(); i++) {
                        output.set(i + 3, inp.get(i));
                    }
                    
                    if (poCounter.isRowNumber() || poCounter.isDenseRank()) {
                        output.set(2, getLocalCounter());
                        incrementSparkCounter();
                        incrementLocalCounter();
                    } else if (!poCounter.isDenseRank()) {
                        int positionBag = inp.getAll().size()-1;
                        if (inp.getType(positionBag) == DataType.BAG) {
                            sizeBag = ((org.apache.pig.data.DefaultAbstractBag)
                                    inp.get(positionBag)).size();
                        }
                        
                        output.set(2, getLocalCounter());
                        
                        addToSparkCounter(sizeBag);
                        addToLocalCounter(sizeBag);
                    }
                    
                    output.set(0, index);
                    output.set(1, getSparkCounter());
                    listOutput.add(output);
                }
            } catch(ExecException e) {
                throw new RuntimeException(e);
            }
            
                    
            return listOutput.iterator();
        }
        
        private long getLocalCounter() {
            return localCount;
        }
        
        private long incrementLocalCounter() {
            return localCount++;
        }
        
        private long addToLocalCounter(long amount) {
            return localCount += amount;
        }
        
        private long getSparkCounter() {
            return sparkCount;
        }
        
        private long incrementSparkCounter() {
            return sparkCount++;
        }
        
        private long addToSparkCounter(long amount) {
            return sparkCount += amount;
        }
    }
}
