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
import org.apache.pig.LoadFunc;

/**
 * Implementing this interface indicates to Pig that a given loader can be 
 * used by a sampling loader.  The requirement for this is that the loader
 * can handle a getNext() call without knowing the position in the file.
 * This will not be the case for loaders that handle structured data such
 * as XML where they must start at the beginning of the file in order to 
 * understand their position.  Record oriented loaders such as PigStorage
 * can handle this by seeking to the next record delimiter and starting
 * from that point.  Another requirement is that the loader be able to 
 * skip or seek in its input stream.
 */
public abstract class SamplableLoader extends LoadFunc {
    
    /**
     * Skip ahead in the input stream.
     * @param n number of bytes to skip
     * @return number of bytes actually skipped.  The return semantics are
     * exactly the same as {@link java.io.InpuStream#skip(long)}
     */
    public abstract long skip(long n) throws IOException;
    
    /**
     * Get the current position in the stream.
     * @return position in the stream.
     */
    public abstract long getPosition() throws IOException;
    
    /**
     * Get the next tuple from the stream starting from the current 
     * read position.
     * The loader implementation should not assume that current read position 
     * in the stream is at the beginning of a record since this method is called
     * for sampling and the current read position in the stream could be anywhere
     * in the stream. 
     * @return the next tuple from underlying input stream or null if there are no more tuples
     * to be processed.
     */
    public abstract Tuple getSampledTuple() throws IOException;
}
