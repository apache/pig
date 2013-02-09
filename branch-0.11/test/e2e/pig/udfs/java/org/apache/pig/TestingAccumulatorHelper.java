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
package org.apache.pig;

import java.io.IOException;

import org.apache.pig.data.Tuple;

public class TestingAccumulatorHelper extends AccumulatorEvalFunc<Integer> implements TerminatingAccumulator<Integer> {
    public boolean earlyTerminate = false;
    public int accumulates = 0;

    public TestingAccumulatorHelper(String earlyTerminate) {
        this.earlyTerminate = Boolean.parseBoolean(earlyTerminate);
    }

    public void accumulate(Tuple input) throws IOException {
        accumulates++;
    }

    public Integer getValue() {
        return accumulates;
    }

    public void cleanup() {
        accumulates = 0;
    }

    public boolean isFinished() {
        return earlyTerminate;
    }
}
