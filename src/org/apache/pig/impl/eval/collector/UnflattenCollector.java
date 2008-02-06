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
package org.apache.pig.impl.eval.collector;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;


public class UnflattenCollector extends DataCollector {
    DataBag bag;
    
    public UnflattenCollector(DataCollector successor){
        super(successor);
    }
    
    @Override
    public void add(Datum d) {
        if (inTheMiddleOfBag){
            if (checkDelimiter(d)){
                successor.add(bag);
            }else{
                if (d instanceof Tuple){
                    bag.add((Tuple)d);
                }else{
                    bag.add(new Tuple(d));
                }
            }
        }else{
            if (checkDelimiter(d)){
                //Bag must have started now
                bag = BagFactory.getInstance().newDefaultBag();
            }else{
                successor.add(d);
            }
        }
    }
    
    

}
