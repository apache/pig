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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;


/**
 * Implementing this interface indicates to Pig that a given loader
 * can be used for MergeJoin. The position as represented by the
 * WritableComparable object is stored in the index created by
 *  MergeJoin sampling MR job to get an ordered sequence of splits.
 * This is necessary when the sort key spans multiple splits.
 * @since Pig 0.7
 */
public interface OrderedLoadFunc {

    /**
     * The WritableComparable object returned will be used to compare
     * the position of different splits in an ordered stream
     * @param split
     * @return WritableComparable representing the position of the split in input
     * @throws IOException
     */
    public WritableComparable<?> getSplitComparable(InputSplit split) 
    throws IOException;

}
