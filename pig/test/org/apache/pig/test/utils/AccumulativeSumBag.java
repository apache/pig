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

/**
 * This class is for testing of accumulator udfs
 *
 */
public class AccumulativeSumBag extends EvalFunc<String> implements Accumulator<String>
{

    StringBuffer sb;

    public AccumulativeSumBag() {
    }

    public void accumulate(Tuple tuple) throws IOException {
        DataBag databag = (DataBag)tuple.get(0);
        if(databag == null)
            return;
        
        if (sb == null) {
            sb = new StringBuffer();
        }
        
        Iterator<Tuple> iterator = databag.iterator();
        while(iterator.hasNext()) {
            Tuple t = iterator.next();
            if (t.size()>1 && t.get(1) == null) {
                continue;
            }
                        
               sb.append(t.toString());
        }
    }

    public String getValue() {
        if (sb != null && sb.length()>0) {
            return sb.toString();
        }
        return null;
    }
    
    public void cleanup() {
        sb = null;
    }

    public String exec(Tuple tuple) throws IOException {
        throw new IOException("exec() should not be called");
    }
}
                                                                                 