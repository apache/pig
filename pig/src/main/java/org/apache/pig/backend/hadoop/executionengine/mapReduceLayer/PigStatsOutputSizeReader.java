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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * Interface to implement when you want to customize the way of computing the
 * size of output in PigStats. Since not every storage is file-based (e.g.
 * HBaseStorage), the output size cannot always be computed as the total size of
 * output files.
 *
 * @see FileBasedOutputSizeReader
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PigStatsOutputSizeReader {

    static final String OUTPUT_SIZE_READER_KEY = "pig.stats.output.size.reader";
    static final String OUTPUT_SIZE_READER_UNSUPPORTED = "pig.stats.output.size.reader.unsupported";

    /**
     * Returns whether the given PSStore is supported by this output size reader
     * or not.
     * @param sto POStore
     * @param conf Configuration
     */
    public boolean supports(POStore sto, Configuration conf);

    /**
     * Returns the size of output in bytes. If the size of output cannot be
     * computed for any reason, -1 should be returned.
     * @param sto POStore
     * @param conf configuration
     */
    public long getOutputSize(POStore sto, Configuration conf) throws IOException;
}
