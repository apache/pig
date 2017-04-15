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

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the StoreFuncMetadataWrapper properly wraps objects that implement both
 * StoreFuncInterface and StoreMetadata
 */
public class TestStoreFuncMetadataWrapper {

    // Asserts that each call to a Wrapper invokes the correct method on the Wrappee.
    @Test
    public void testSucess() throws IOException {
        DummyStoreMetadata storeFunc = new DummyStoreMetadata();
        MockWrapper wrapper = new MockWrapper(storeFunc);

        wrapper.storeSchema(null, null, null);
        assertEquals("storeSchema", storeFunc.getLastMethodCalled());

        wrapper.storeStatistics(null, null, null);
        assertEquals("storeStatistics", storeFunc.getLastMethodCalled());

        wrapper.relToAbsPathForStoreLocation(null, null);
        assertEquals("relToAbsPathForStoreLocation", storeFunc.getLastMethodCalled());

        wrapper.getOutputFormat();
        assertEquals("getOutputFormat", storeFunc.getLastMethodCalled());

        wrapper.setStoreLocation(null, null);
        assertEquals("setStoreLocation", storeFunc.getLastMethodCalled());

        wrapper.checkSchema(null);
        assertEquals("checkSchema", storeFunc.getLastMethodCalled());

        wrapper.prepareToWrite(null);
        assertEquals("prepareToWrite", storeFunc.getLastMethodCalled());

        wrapper.putNext(null);
        assertEquals("putNext", storeFunc.getLastMethodCalled());

        wrapper.setStoreFuncUDFContextSignature(null);
        assertEquals("setStoreFuncUDFContextSignature", storeFunc.getLastMethodCalled());

        wrapper.cleanupOnSuccess(null, null);
        assertEquals("cleanupOnSuccess", storeFunc.getLastMethodCalled());

        wrapper.cleanupOnFailure(null, null);
        assertEquals("cleanupOnFailure", storeFunc.getLastMethodCalled());

        NoSuchElementException e = null;
        try {
            storeFunc.getLastMethodCalled();
        } catch (NoSuchElementException ex) {
            e = ex;
        }

        assertNotNull("The wrapped class had more method invokations than it should have", e);
    }

    // Asserts that each call to an improperly initialized Wrapper fails.
    @Test
    public void testError() throws IOException {
        MockWrapper wrapper = new MockWrapper(); // <-- !! storeFunc not set !!

        IllegalArgumentException e = null;
        try {
            wrapper.storeSchema(null, null, null);
        } catch (IllegalArgumentException ex) {
            e = ex;
        }

        assertNotNull("A useful exception should have been thrown when a method is called on an "
            + "improperly initialized StoreFuncMetadataWrapper", e);
        assertTrue("The method name that caused the problem should have been mentioned in the "
            + "exception, since the stack trace gets swallowed when this occurs",
            e.getMessage().contains("storeSchema"));
    }

    private static class MockWrapper extends StoreFuncMetadataWrapper {
        private MockWrapper() { }

        private MockWrapper(StoreMetadata storeMetadata) {
            setStoreFunc(storeMetadata);
        }
    }

    private static class DummyStoreMetadata extends TestStoreFuncWrapper.DummyStoreFunc
                                          implements StoreMetadata, StoreFuncInterface {
        @Override
        public void storeStatistics(ResourceStatistics stats, String location, Job job)
        throws IOException {
            setLastMethodCalled();
        }

        @Override
        public void storeSchema(ResourceSchema schema, String location, Job job)
        throws IOException {
            setLastMethodCalled();
        }
    }
}
