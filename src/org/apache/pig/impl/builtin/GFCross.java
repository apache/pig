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

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class GFCross extends EvalFunc<DataBag> {
    int numInputs, myNumber, numGroupsPerInput;
    BagFactory mBagFactory = BagFactory.getInstance();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    public static int DEFAULT_PARALLELISM = 96;

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try{
            numInputs = (Integer)input.get(0);
            myNumber = (Integer)input.get(1);
        
            DataBag output = mBagFactory.newDefaultBag();
            
            numGroupsPerInput = (int) Math.ceil(Math.pow(DEFAULT_PARALLELISM, 1.0/numInputs));
            int numGroupsGoingTo = (int) Math.pow(numGroupsPerInput,numInputs - 1);
                
            int[] digits = new int[numInputs];
            for (int i=0; i<numInputs; i++){
                if (i == myNumber){
                    Random r = new Random(System.currentTimeMillis());
                    digits[i] = r.nextInt(numGroupsPerInput);
                }else{
                    digits[i] = 0;
                }
            }
                
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
