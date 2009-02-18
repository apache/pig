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

import org.apache.pig.PigException;


public class DiscreteProbabilitySampleGenerator {
    Random rGen;
    float[] probVec;
    float epsilon = 0.00001f;
    
    public DiscreteProbabilitySampleGenerator(long seed, float[] probVec) throws MalFormedProbVecException{
        rGen = new Random(seed);
        float sum = 0.0f;
        for (float f : probVec) {
            sum += f;
        }
        if(1-epsilon<=sum && sum<=1+epsilon) 
            this.probVec = probVec;
        else {
            int errorCode = 2122;
            String message = "Sum of probabilities should be one";
            throw new MalFormedProbVecException(message, errorCode, PigException.BUG);
        }
    }
    
    public DiscreteProbabilitySampleGenerator(float[] probVec) throws MalFormedProbVecException{
        rGen = new Random();
        float sum = 0.0f;
        for (float f : probVec) {
            sum += f;
        }
        if(1-epsilon<=sum && sum<=1+epsilon) 
            this.probVec = probVec;
        else {
            int errorCode = 2122;
            String message = "Sum of probabilities should be one";
            throw new MalFormedProbVecException(message, errorCode, PigException.BUG);
        }
    }
    
    public int getNext(){
        double toss = rGen.nextDouble();
        // if the uniformly random number that I generated
        // is in the probability range for a given parition,
        // pick that parition
        // For some sample item which occurs only in partitions
        // 1 and 2
        // say probVec[1] = 0.3
        // and probVec[2] = 0.7
        // if our coin toss generate < 0.3, we pick 1 otherwise
        // we pick 2
        for(int i=0;i<probVec.length;i++){
            toss -= probVec[i];
            if(toss<=0.0)
                return i;
        }
        return -1;
    }
    
    public static void main(String[] args) throws MalFormedProbVecException {
        float[] vec = { 0, 0.3f, 0.2f, 0, 0, 0.5f };
        DiscreteProbabilitySampleGenerator gen = new DiscreteProbabilitySampleGenerator(11317, vec);
        CountingMap<Integer> cm = new CountingMap<Integer>();
        for(int i=0;i<100;i++){
            cm.put(gen.getNext(), 1);
        }
        cm.display();
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return Arrays.toString(probVec);
    }
    
    
}
