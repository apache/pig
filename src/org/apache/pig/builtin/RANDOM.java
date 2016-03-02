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
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * Return a random double value.  Whatever arguments are passed to this UDF
 * are ignored.
 */
@Nondeterministic
public class RANDOM extends EvalFunc<Double>{
    private Random r = null;

    public RANDOM() {
    }

    public RANDOM(String seed) {
        r = new Random(Long.parseLong(seed));
    }

    @Override
    public Double exec(Tuple input) throws IOException {
        if( r == null ) {
            int jobidhash = PigMapReduce.sJobConfInternal.get().get(MRConfiguration.JOB_ID).hashCode();
            int taskIndex = Integer.valueOf(PigMapReduce.sJobConfInternal.get().get(PigConstants.TASK_INDEX));
            r = new Random(((long) jobidhash) << 32 | (taskIndex & 0xffffffffL));
        }
        return r.nextDouble();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.DOUBLE));
    }
}
