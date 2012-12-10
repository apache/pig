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

/**
 * This is an interface which, if implemented, allows an Accumulator
 * function to signal that it can terminate early. Certain classes of
 * UDF to do not need access to an entire set of data in order to
 * finish processing. A model example is {org.apache.pig.builtin.IsEmpty}. IsEmpty
 * can be Accumulative as if it receives even one line, it knows that
 * it is not empty. Another example might be a UDF which does streaming
 * analysis, and once a given stream matches a criteria, can terminate
 * without needing any further analysis.
 */
public interface TerminatingAccumulator<T> extends Accumulator<T> {
    public boolean isFinished();
}
