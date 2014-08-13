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

package org.apache.pig.test.utils;

import java.io.IOException;
import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class AccumulatorBagCount extends EvalFunc<Integer> implements Accumulator<Integer> {

    int count = 0;

    public AccumulatorBagCount() {
    }

    public void accumulate(Tuple tuple) throws IOException {
        DataBag databag = (DataBag)tuple.get(0);
        if(databag == null)
            return;
        
        Iterator<Tuple> iterator = databag.iterator();
        while(iterator.hasNext()) {
            iterator.next();
            count++;
        }
    }

    public Integer getValue() {
        return new Integer(count);
    }

    public void cleanup() {
        count = 0;
    }

    public Integer exec(Tuple tuple) throws IOException {
        throw new IOException("exec() should not be called.");
    }
}
                                                                                 