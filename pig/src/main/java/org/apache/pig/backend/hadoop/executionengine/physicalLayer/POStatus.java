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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

/**
 * POStatus is a set of flags used to communicate the status of Pig's operator
 * pipeline to consumers.
 */
public class POStatus {

    /**
     * STATUS_OK indicates that the pull on the operator pipeline resulted in a
     * valid output.
     */
    public static final byte STATUS_OK = 0;

    /**
     * STATUS_NULL indicates that no output was produced, but there may be more
     * results. This can happen if a value is filtered out or an empty bag is
     * flattened. A caller will typically ignore the output and try again after
     * seeing STATUS_NULL.
     *
     * This does NOT indicate that the output is the value 'null' (which is
     * possible in expressions). This is represented as 'null' with STATUS_OK.
     */
    public static final byte STATUS_NULL = 1;

    /**
     * STATUS_ERR indicates that there was a problem while trying to produce a
     * result. This should be remembered and fed back to the user.
     */
    public static final byte STATUS_ERR = 2;

    /**
     * STATUS_EOP indicates that no output was produced, and no further outputs
     * will be produced (e.g. all attached inputs have been consumed or a limit
     * has reached its threshold). A caller will typically terminate or attach
     * new inputs on seeing this status.
     */
    public static final byte STATUS_EOP = 3;

    /**
     * This is currently only used in communications between ExecutableManager
     * and POStream. It indicates the end of Streaming output (i.e. output from
     * streaming binary).
     */
    public static final byte STATUS_EOS = 4;

    /**
     * Successful processing of a batch. This is used for accumulative UDFs.
     */
    public static final byte STATUS_BATCH_OK = 5;

    /**
     * This signals that an accumulative UDF has already finished.
     */
    public static final byte STATUS_EARLY_TERMINATION = 6;
}
