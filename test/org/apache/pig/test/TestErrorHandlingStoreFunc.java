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
package org.apache.pig.test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pig.CounterBasedErrorHandler;
import org.apache.pig.ErrorHandling;
import org.apache.pig.ExecType;
import org.apache.pig.ErrorHandler;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * This class contains Unit tests for store func which has a certain error
 * threshold set.
 * 
 */
public class TestErrorHandlingStoreFunc {

    private static PigServer pigServer;
    private File tempDir;

    @Before
    public void setup() throws IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
    }

    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
        tempDir.delete();
    }

    public static class TestErroroneousStoreFunc extends PigStorage implements
            ErrorHandling {
        protected static AtomicLong COUNTER = new AtomicLong();

        @Override
        public void putNext(Tuple f) throws IOException {
            long count = COUNTER.incrementAndGet();
            super.putNext(f);
            if (count % 3 == 0) {
                throw new RuntimeException("Throw error for test");
            }
        }

        @Override
        public ErrorHandler getErrorHandler() {
            return new CounterBasedErrorHandler();
        }
    }

    public static class TestErroroneousStoreFunc2 extends PigStorage implements
            ErrorHandling {
        protected static AtomicLong COUNTER = new AtomicLong();

        @Override
        public void putNext(Tuple f) throws IOException {
            long count = COUNTER.incrementAndGet();
            super.putNext(f);
            if (count % 3 == 0) {
                throw new RuntimeException("Throw error for test");
            }
        }

        @Override
        public ErrorHandler getErrorHandler() {
            return new CounterBasedErrorHandler();
        }
    }

    /**
     * Test Pig job succeeds even with errors within threshold
     * 
     */
    @Test
    public void testStorerWithErrorInLimit() throws Exception {
        updatePigProperties(true, 3L, 0.4);
        runTest(JOB_STATUS.COMPLETED);
    }

    /**
     * Test Pig job fails if errors exceed min errors and threshold
     */
    @Test
    public void testStorerWithErrorOutExceedingLimit() throws Exception {
        updatePigProperties(true, 2L, 0.3);
        runTest(JOB_STATUS.FAILED);
    }

    /**
     * Test Pig Job fails on error if the config is set to false
     * 
     * @throws Exception
     */
    @Test
    public void testStorerWithConfigNotEnabled() throws Exception {
        updatePigProperties(false, 3L, 0.3);
        runTest(JOB_STATUS.FAILED);
    }

    /**
     * Test Pig Job with multiple stores.
     * 
     * @throws Exception
     */
    @Test
    public void testMultiStore() throws Exception {
        updatePigProperties(true, 3L, 0.4);
        pigServer.getPigContext().getProperties()
                .put(PigConfiguration.PIG_OPT_MULTIQUERY, "" + true);
        Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();
        // Create input dataset
        int rows = 10;
        for (int i = 0; i < rows; i++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(i);
            t.append("a" + i);
            list.add(t);
        }
        data.set("in", "id:int,name:chararray", list);
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
        String storeAQuery = "store A into '" + tempDir.getAbsolutePath()
                + "/output' using " + TestErroroneousStoreFunc.class.getName()
                + "();";
        pigServer.registerQuery(storeAQuery);
        pigServer.registerQuery("B = FILTER A by id >0;");
        String storeBQuery = "store B into '" + tempDir.getAbsolutePath()
                + "/output2' using "
                + TestErroroneousStoreFunc2.class.getName() + "();";
        pigServer.registerQuery(storeBQuery);
        if (pigServer.executeBatch().get(0).getStatus() != JOB_STATUS.COMPLETED) {
            throw new RuntimeException("Job failed", pigServer.executeBatch()
                    .get(0).getException());
        }
    }

    private void runTest(JOB_STATUS expectedJobStatus) throws Exception {
        Data data = Storage.resetData(pigServer);
        final Collection<Tuple> list = Lists.newArrayList();
        // Create input dataset
        int rows = 10;
        for (int i = 0; i < rows; i++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(i);
            t.append("a" + i);
            list.add(t);
        }
        data.set("in", "id:int,name:chararray", list);

        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'in' USING mock.Storage();");
        String storeQuery = "store A into '" + tempDir.getAbsolutePath()
                + "/output' using " + TestErroroneousStoreFunc.class.getName()
                + "();";
        pigServer.registerQuery(storeQuery);

        if (pigServer.executeBatch().get(0).getStatus() != expectedJobStatus) {
            throw new RuntimeException("Job did not reach the expected status"
                    + pigServer.executeBatch().get(0).getStatus());
        }
    }

    private void updatePigProperties(boolean allowErrors, long minErrors,
            double errorThreshold) {
        Properties properties = pigServer.getPigContext().getProperties();
        properties.put(PigConfiguration.PIG_ALLOW_STORE_ERRORS,
                Boolean.toString(allowErrors));
        properties.put(PigConfiguration.PIG_ERRORS_MIN_RECORDS,
                Long.toString(minErrors));
        properties.put(PigConfiguration.PIG_ERROR_THRESHOLD_PERCENT,
                Double.toString(errorThreshold));
    }
}
