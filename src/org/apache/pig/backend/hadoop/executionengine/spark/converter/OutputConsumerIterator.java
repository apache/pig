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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.Tuple;

abstract class OutputConsumerIterator implements java.util.Iterator<Tuple> {
    private final java.util.Iterator<Tuple> input;
    private Result result = null;
    private boolean returned = true;
    private boolean done = false;

    OutputConsumerIterator(java.util.Iterator<Tuple> input) {
        this.input = input;
    }

    abstract protected void attach(Tuple tuple);

    abstract protected Result getNextResult() throws ExecException;

    /**
     * Certain operators may buffer the output.
     * We need to flush the last set of records from such operators,
     * when we encounter the last input record, before calling
     * getNextTuple() for the last time.
     */
    abstract protected void endOfInput();

    private void readNext() {
        while (true) {
            try {
                // result is set in hasNext() call and returned
                // to the user in next() call
                if (result != null && !returned) {
                    return;
                }

                if (result == null) {
                    if (!input.hasNext()) {
                        done = true;
                        return;
                    }
                    Tuple v1 = input.next();
                    attach(v1);
                }

                if (!input.hasNext()) {
                    endOfInput();
                }

                result = getNextResult();
                returned = false;
                switch (result.returnStatus) {
                    case POStatus.STATUS_OK:
                        returned = false;
                        break;
                    case POStatus.STATUS_NULL:
                        returned = true;
                        break;
                    case POStatus.STATUS_EOP:
                        done = !input.hasNext();
                        if (!done) {
                            result = null;
                        }
                        break;
                    case POStatus.STATUS_ERR:
                        throw new RuntimeException("Error while processing " + result);
                }

            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean hasNext() {
        readNext();
        return !done;
    }

    @Override
    public Tuple next() {
        readNext();
        if (done) {
            throw new RuntimeException("Past the end. Call hasNext() before calling next()");
        }
        if (result == null || result.returnStatus != POStatus.STATUS_OK) {
            throw new RuntimeException("Unexpected response code from operator: "
                    + result);
        }
        returned = true;
        return (Tuple) result.result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}