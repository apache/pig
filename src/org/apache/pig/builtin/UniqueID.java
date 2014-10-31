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
package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * UniqueID generates a unique id for each records in the job. This unique id is
 * stable in task retry. Any arguments to the function are ignored.
 * Example:
 *      A = load 'mydata' as (name);
 *      B = foreach A generate name, UniqueID();
 * UniqueID takes the form "index-sequence"
 */
public class UniqueID extends EvalFunc<String> {
    long sequence = 0;
    @Override
    public String exec(Tuple input) throws IOException {
        String taskIndex = PigMapReduce.sJobConfInternal.get().get(PigConstants.TASK_INDEX);    
        String sequenceId = taskIndex + "-" + Long.toString(sequence);
        sequence++;
        return sequenceId;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("uniqueid", DataType.CHARARRAY));
    }
}
