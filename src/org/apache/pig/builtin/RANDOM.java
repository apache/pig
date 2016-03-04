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
import org.apache.pig.StaticDataCleanup;
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

            // XOR-ing 3 separate values
            // |<-----32 bits---->|<----32 bits----->|
            // |-jobidhash(int)---|-jobidhash(int)---|
            // |        |---taskIndex(int)--|
            // |----------seedUniquifier (long)------|
            //          |                            |
            //          |<-- Only 48 bits used ----->|
            //          |    by java.util.Random     |
            //          |                            |
            //
            // Reason for repeating jobidhash and shifting taskIndex is, seed
            // too close to each others would produce very similar values.
            //
            // Goal of this method is to produce a pseudo-random values that
            // would
            // (1) Produce a same sequence of peusdo-random variables for attempts from same jobid/vertexid and taskid
            // (2) When taskid, jobid, or vertexid(tez) differ, they should produce a different random sequence
            // (3) When Random is called more than once inside the script, they should also produce different random values
            //     e.g. B = FOREACH A generate RANDOM(), RANDOM();
            //
            r = new Random( (((long) jobidhash) << 32 | (jobidhash &  0xffffffffL))  ^ ((long) taskIndex << 16) ^ seedUniquifier);

            // L'Ecuyer, "Tables of Linear Congruential Generators of
            // Different Sizes and Good Lattice Structure", 1999
            seedUniquifier *= 4292484099903637661L;
        }
        return r.nextDouble();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.DOUBLE));
    }

    // Taking the initial seed value from java.util.Random
    private static long seedUniquifier = 8682522807148012L;

    @StaticDataCleanup
    public static void resetSeedUniquifier() {
        seedUniquifier = 8682522807148012L;
    }
}
