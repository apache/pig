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

package org.apache.pig.test.udf.storefunc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class StringStore extends PigStorage {

    /**
     * return each line from input as one field in the tuple in
     * each getNext()
     */
    @Override
    public Tuple getNext() throws IOException {
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        try{
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }                                                                                           
            Text value = (Text) in.getCurrentValue();
            String line = value.toString();
            t.set(0, line);
        }catch(Exception e){ throw new RuntimeException(e);}
        return t;
    }
    
    @Override
    public void putNext(Tuple f) throws IOException {
        byte[] output = (f.toString()).getBytes("utf8");
        Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
        t.set(0, new DataByteArray(output));
        try {
            writer.write(null, t);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    
}
    
