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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;


public class GFCross extends EvalFunc<DataBag> {
    private int numInputs, myNumber, numGroupsPerInput, numGroupsGoingTo;
    private BagFactory mBagFactory = BagFactory.getInstance();
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private int parallelism = 0;
    private Random r = new Random();

    static private final int DEFAULT_PARALLELISM = 96;
    

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (parallelism == 0) {
            parallelism = DEFAULT_PARALLELISM;
            Configuration cfg = UDFContext.getUDFContext().getJobConf();
            if (cfg != null) {
                String s = cfg.get("mapred.reduce.tasks");
                if (s == null) {
                    throw new IOException("Unable to determine parallelism from job conf");
                }
                parallelism = Integer.valueOf(s);
            }

            numInputs = (Integer)input.get(0);
            myNumber = (Integer)input.get(1);
        
            numGroupsPerInput = (int) Math.ceil(Math.pow(parallelism, 1.0/numInputs));
            numGroupsGoingTo = (int) Math.pow(numGroupsPerInput,numInputs - 1);
        }

        DataBag output = mBagFactory.newDefaultBag();

        try{
               
            int[] digits = new int[numInputs];
            digits[myNumber] = r.nextInt(numGroupsPerInput);

            for (int i=0; i<numGroupsGoingTo; i++){
                output.add(toTuple(digits));
                next(digits);
            }            
    
            return output;
        }catch(ExecException e){
            throw e;
        }
    }
    
    private Tuple toTuple(int[] digits) throws IOException, ExecException{
        Tuple t = mTupleFactory.newTuple(numInputs);
        for (int i=0; i<numInputs; i++){
            t.set(i, digits[i]);
        }
        return t;
    }
    
    private void next(int[] digits){
        for (int i=0; i<numInputs; i++){
            if (i== myNumber)
                continue;
            else{
                if (digits[i] == numGroupsPerInput - 1){
                    digits[i] = 0;
                }else{
                    digits[i]++;
                    break;
                }
            }
        }
    }

}
