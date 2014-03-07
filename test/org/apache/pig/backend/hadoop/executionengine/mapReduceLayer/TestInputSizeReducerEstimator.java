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

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.test.PigStorageWithStatistics;
import org.apache.pig.test.TestJobControlCompiler;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestInputSizeReducerEstimator {

    private static final Configuration CONF = new Configuration(false);

    @Test
    public void testGetInputSizeFromFs() throws Exception {
        long size = 2L * 1024 * 1024 * 1024;
        Assert.assertEquals(size, InputSizeReducerEstimator.getTotalInputFileSize(
                CONF, Lists.newArrayList(createPOLoadWithSize(size, new PigStorage())),
                new org.apache.hadoop.mapreduce.Job(CONF)));

        Assert.assertEquals(size, InputSizeReducerEstimator.getTotalInputFileSize(
                CONF,
                Lists.newArrayList(createPOLoadWithSize(size, new PigStorageWithStatistics())),
                new org.apache.hadoop.mapreduce.Job(CONF)));

        Assert.assertEquals(size * 2, InputSizeReducerEstimator.getTotalInputFileSize(
                CONF,
                Lists.newArrayList(
                        createPOLoadWithSize(size, new PigStorage()),
                        createPOLoadWithSize(size, new PigStorageWithStatistics())),
                        new org.apache.hadoop.mapreduce.Job(CONF)));

        // Negative test - PIG-3754
        POLoad poLoad = createPOLoadWithSize(size, new PigStorage());
        poLoad.setLFile(new FileSpec("hbase://users", null));

        Assert.assertEquals(-1, InputSizeReducerEstimator.getTotalInputFileSize(
                CONF,
                Collections.singletonList(poLoad),
                new org.apache.hadoop.mapreduce.Job(CONF)));
    }

    @Test
    public void testGetInputSizeFromLoader() throws Exception {
        long size = 2L * 1024 * 1024 * 1024;
        Assert.assertEquals(size, InputSizeReducerEstimator.getInputSizeFromLoader(
                createPOLoadWithSize(size, new PigStorageWithStatistics()),
                new org.apache.hadoop.mapreduce.Job(CONF)));
    }

    private static POLoad createPOLoadWithSize(long size, LoadFunc loadFunc) throws Exception {
        return TestJobControlCompiler.createPOLoadWithSize(size, loadFunc);
    }
}
