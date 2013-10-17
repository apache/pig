/**
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

package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class FileInputHandler implements InputHandler {
    MRInput mrInput;
    KeyValueReader reader;

    protected TupleFactory tf = TupleFactory.getInstance();
    
    @Override
    public boolean next() throws IOException {
        return reader.next();
    }

    @Override
    public Tuple getCurrentTuple() throws IOException {
        Tuple readTuple = (Tuple) reader.getCurrentValue();
        return tf.newTupleNoCopy(readTuple.getAll());
    }

    @Override
    public void initialize(Configuration conf, Map<String, LogicalInput> inputs) throws IOException {
        Input input = inputs.get("PigInput");
        if (input == null || !(input instanceof MRInput)){
            throw new PigException("Incorrect Input class. Expected sub-type of MRInput," +
                    " got " + input.getClass().getName());
        }

        mrInput = (MRInput) input;
        reader = mrInput.getReader();
    }
}
