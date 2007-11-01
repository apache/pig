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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.Datum;


public class FindQuantiles extends EvalFunc<DataBag>{
	
	/**
	 * first field in the input tuple is the number of quantiles to generate
	 * second field is the *sorted* bag of samples
	 */
	
	@Override
	public void exec(Tuple input, DataBag output) throws IOException {
		int numQuantiles = input.getAtomField(0).numval().intValue();
		DataBag samples = input.getBagField(1);
		
		int numSamples = samples.cardinality();
		
		int toSkip = numSamples / numQuantiles;
		
		int i=0, nextQuantile = 0;
		Iterator<Datum> iter = samples.content();
		while (iter.hasNext()){
			Tuple t = (Tuple)iter.next();
			if (i==nextQuantile){
				output.add(t);
				nextQuantile+=toSkip+1;
			}
			i++;
		}
	}
}
