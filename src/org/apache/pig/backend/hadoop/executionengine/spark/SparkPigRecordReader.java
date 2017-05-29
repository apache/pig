/**
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

package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigRecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

/**
 * Record reader for Spark mode - handles SparkPigSplit
 */
public class SparkPigRecordReader extends PigRecordReader {


    /**
     * @param inputformat
     * @param pigSplit
     * @param loadFunc
     * @param context
     * @param limit
     */
    public SparkPigRecordReader(InputFormat<?, ?> inputformat, PigSplit pigSplit, LoadFunc loadFunc, TaskAttemptContext context, long limit) throws IOException, InterruptedException {
        super(inputformat, pigSplit, loadFunc, context, limit);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        SparkPigSplit sparkPigSplit = (SparkPigSplit)split;
        super.initialize(sparkPigSplit.getWrappedPigSplit(), context);
    }
}
