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
package org.apache.pig.tutorial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * For each n-gram, we have a set of (hour, count) pairs.
 * 
 * This function reads the set and retains those hours with above
 * above mean count, and calculates the score of each retained hour as the 
 * multiplier of the count of the hour over the standard deviation.
 * 
 * A score greater than 1.0 indicates the frequency of this n-gram 
 * in this particular hour is at least one standard deviation away 
 * from the average frequency among all hours
 */

public class ScoreGenerator extends EvalFunc<DataBag> {

  private static double computeMean(List<Long> counts) {
    int numCounts = counts.size();
    
    // compute mean
    double mean = 0.0;
    for (Long count : counts) {
      mean += ((double) count) / ((double) numCounts);
    }
    
    return mean;
  }
  
  private static double computeSD(List<Long> counts, double mean) {
    int numCounts = counts.size();
    
    // compute deviation
    double deviation = 0.0;
    for (Long count : counts) {
      double d = ((double) count) - mean;
      deviation += d * d / ((double) numCounts);
    }
    
    return Math.sqrt(deviation);
  }

  public void exec(Tuple arg0, DataBag arg1) throws IOException {
    DataBag input = arg0.getBagField(0);
    
    Map<String, Long> pairs = new HashMap<String, Long>();
    List<Long> counts = new ArrayList<Long> ();

    Iterator<Tuple> it = input.iterator();
    while (it.hasNext()) {
      Tuple t = it.next();
      String hour = t.getAtomField(1).strval();
      Long count = t.getAtomField(2).longVal();
      pairs.put(hour, count);
      counts.add(count);
    }
    
    double mean = computeMean(counts);
    double standardDeviation = computeSD(counts, mean);

    Iterator<String> it2 = pairs.keySet().iterator();
    while (it2.hasNext()) {
      String hour = it2.next();
      Long count = pairs.get(hour);
      if ( count > mean ) {
        Tuple t = new Tuple();
        t.appendField(new DataAtom(hour));
        t.appendField(new DataAtom( ((double) count - mean) / standardDeviation )); // the score
        t.appendField(new DataAtom(count));
        t.appendField(new DataAtom(mean));
        arg1.add(t);
      }
    }
    
  }
}
