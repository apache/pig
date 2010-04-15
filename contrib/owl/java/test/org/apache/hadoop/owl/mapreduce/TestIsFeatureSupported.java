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
package org.apache.hadoop.owl.mapreduce;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.mapreduce.OwlPartitionValues;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.data.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 * Dummy implementation of an abstract storage driver class
 * that reports that it DOES support all features
 */
class OwlInputYesSD extends OwlInputStorageDriver {
    public InputFormat<BytesWritable, Tuple> getInputFormat(OwlLoaderInfo loaderInfo) { return null; }
    public boolean isFeatureSupported(OwlInputFormat.OwlOperation operation) throws IOException { return true; }
    public void setInputPath(JobContext jobContext, String location) throws IOException {}
    public void setOriginalSchema(JobContext jobContext, OwlSchema schema) throws IOException {}
    public void setOutputSchema(JobContext jobContext, OwlSchema schema) throws IOException {}
    public void setPartitionValues(JobContext jobContext, OwlPartitionValues partitionValues) throws IOException {}

}

/**
 * Dummy implementation of an abstract storage driver class
 * that reports that it DOES NOT support all features
 */
class OwlInputNoSD extends OwlInputStorageDriver {
    public InputFormat<BytesWritable, Tuple> getInputFormat(OwlLoaderInfo loaderInfo) { return null; }
    public boolean isFeatureSupported(OwlInputFormat.OwlOperation operation) throws IOException { return false; }
    public void setInputPath(JobContext jobContext, String location) throws IOException {}
    public void setOriginalSchema(JobContext jobContext, OwlSchema schema) throws IOException {}
    public void setOutputSchema(JobContext jobContext, OwlSchema schema) throws IOException {}
    public void setPartitionValues(JobContext jobContext, OwlPartitionValues partitionValues) throws IOException {}
}

/**
 * Test OwlInputFormat.isFeatureSupported():
 *
 * Create several mocked partitions with storage drivers supporting
 * or not supporting given feature, assemble partitions into a mock
 * OwlJobInfo and call OwlInputFormat.isFeatureSupported()
 */

public class TestIsFeatureSupported extends OwlTestCase {

    /**
     * Iterate over all OwlOperations (currently only one)
     * and test isFeatureSupported() consolidation logic on each
     */
    @Test
    public static void testIsFeatureSupported() {
        for (OwlInputFormat.OwlOperation feature: OwlInputFormat.OwlOperation.values()) {
            testFeature(feature);
        }
    }

    /**
     * Test OwlInputFormat.isFeatureSupported() for a given feature by constructing
     * the OwlJobInfo with the following permutations of partitions supporting (true)
     * and not supporting (false) the feature:
     *
     * 1. true
     * 2. false
     * 3. true, true, true
     * 4. true, false, true
     * 5. false, false, false
     *
     * @param feature OwlOperation to be tested
     */
    private static void testFeature(OwlInputFormat.OwlOperation feature) {
        boolean[] list1 = {true};
        testCombination(feature, list1, true);
        boolean[] list2 = {false};
        testCombination(feature, list2, false);
        boolean[] list3 = {true, true, true};
        testCombination(feature, list3, true);
        boolean[] list4 = {true, false, true};
        testCombination(feature, list4, false);
        boolean[] list5 = {false, false, false};
        testCombination(feature, list5, false);
    }

    /**
     * Test a given combination of support features.
     * @see #testFeature
     *
     * Use OwlInputYesSD and OwlInputNoSD classes and construct OwlTableInputInfo
     * object with a list of partitions with specified feature support.
     * Then - check if OwlInputFormat.isFeatureSupported() returns correct result
     *
     * @param feature OwlOperation to be tested
     * @param list combination to be tested
     * @param result expected result
     */
    private static void testCombination(OwlInputFormat.OwlOperation feature, boolean[] list, boolean result) {
        /*
         *  Mock a list of OwlPartitionInfo given a specified list of partitions
         */
        List<OwlPartitionInfo> opiList = new ArrayList<OwlPartitionInfo>();
        for (boolean aList : list) {
            /*
             * Mock OwlPartitionInfo -> OwlLoaderInfo -> driverClass name
             */
            OwlLoaderInfo oli = mock(OwlLoaderInfo.class);
            String driverClass = (aList ? OwlInputYesSD.class : OwlInputNoSD.class).getName();
            when(oli.getInputDriverClass()).thenReturn(driverClass);
            OwlPartitionInfo opi = mock(OwlPartitionInfo.class);
            when(opi.getLoaderInfo()).thenReturn(oli);
            opiList.add(opi);
        }

        /*
         * Mock OwlTableInputInfo -> OwlJobInfo -> partition list
         */
        OwlJobInfo jobInfo = mock(OwlJobInfo.class);
        when(jobInfo.getPartitions()).thenReturn(opiList);
        OwlTableInputInfo inputInfo = mock(OwlTableInputInfo.class);
        when(inputInfo.getJobInfo()).thenReturn(jobInfo);

        /*
         * Now check if OwlInputFormat.isFeatureSupported() matches provided outcome
         */
        String callName = "OwlInputFormat.isFeatureSupported(partitions:";
        try {
            System.out.println(callName + Arrays.toString(list) + ", " + feature.toString() + ") == " + result);
            assertEquals(callName + Arrays.toString(list) + ", " + feature.toString() + ") failed:",
                    OwlInputFormat.isFeatureSupported(inputInfo, feature), result);
        } catch (IOException e) {
            fail(callName + Arrays.toString(list) + ", " + feature.toString() + ") throws "+e.getMessage());
        }
    }

}

