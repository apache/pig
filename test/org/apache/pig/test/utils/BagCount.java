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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class BagCount extends EvalFunc<Integer> { 

    
    public BagCount() {
    }


    public Integer exec(Tuple tuple) throws IOException {
        DataBag databag = (DataBag)tuple.get(0);
        if(databag == null) {
            return new Integer(0);
        }
        
        int count = 0;

        Iterator<Tuple> iterator = databag.iterator();
        while(iterator.hasNext()) {
            iterator.next();
            count++;
        }
        
        return new Integer(count);
    }
}
                                                                                 