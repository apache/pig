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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiscreteProbabilitySampleGenerator {
    Random rGen;
    float[] probVec;
    float epsilon = 0.00001f;
        
    private static final Log LOG = LogFactory.getLog(DiscreteProbabilitySampleGenerator.class);
    
    public DiscreteProbabilitySampleGenerator(float[] probVec) {
        rGen = new Random();
        float sum = 0.0f;
        for (float f : probVec) {
            sum += f;
        }
        this.probVec = probVec;
        if (1-epsilon > sum || sum > 1+epsilon) { 
            LOG.info("Sum of probabilities should be near one: " + sum);
        }
    }
    
    public int getNext(){
        double toss = rGen.nextDouble();
        // if the uniformly random number that I generated
        // is in the probability range for a given partition,
        // pick that partition
        // For some sample item which occurs only in partitions
        // 1 and 2
        // say probVec[1] = 0.3
        // and probVec[2] = 0.7
        // if our coin toss generate < 0.3, we pick 1 otherwise we pick 2
        int lastIdx = -1;
        for(int i=0;i<probVec.length;i++){
            if (probVec[i] != 0) lastIdx = i;
            toss -= probVec[i];
            if(toss<=0.0)
                return i;
        }        
        return lastIdx;
    }
    
    public static void main(String[] args) {
        float[] vec = { 0, 0.3f, 0.2f, 0, 0, 0.5f };
        DiscreteProbabilitySampleGenerator gen = new DiscreteProbabilitySampleGenerator(vec);
        CountingMap<Integer> cm = new CountingMap<Integer>();
        for(int i=0;i<100;i++){
            cm.put(gen.getNext(), 1);
        }
        cm.display();
    }

    @Override
    public String toString() {
        return Arrays.toString(probVec);
    }
    
    
}
