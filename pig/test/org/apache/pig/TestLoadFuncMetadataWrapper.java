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

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the LoadFuncMetadataWrapper properly wraps objects that extends 
 * LoadFunc and implements LoadtMetadata
 */
public class TestLoadFuncMetadataWrapper {

    // Asserts that each call to a Wrapper invokes the correct method on the Wrappee.
    @Test
    public void testSucess() throws IOException {
        DummyLoadMetadata loadFunc = new DummyLoadMetadata();
        MockWrapper wrapper = new MockWrapper(loadFunc);

        wrapper.getSchema(null, null);
        assertEquals("getSchema", loadFunc.getLastMethodCalled());

        wrapper.getStatistics(null, null);
        assertEquals("getStatistics", loadFunc.getLastMethodCalled());

        wrapper.getPartitionKeys(null, null);
        assertEquals("getPartitionKeys", loadFunc.getLastMethodCalled());

        wrapper.setPartitionFilter(null);
        assertEquals("setPartitionFilter", loadFunc.getLastMethodCalled());

        wrapper.relativeToAbsolutePath(null,null);
        assertEquals("relativeToAbsolutePath", loadFunc.getLastMethodCalled());

        wrapper.setLocation(null,null);
        assertEquals("setLocation", loadFunc.getLastMethodCalled());

        wrapper.getInputFormat();
        assertEquals("getInputFormat", loadFunc.getLastMethodCalled());
        
        wrapper.getLoadCaster();
        assertEquals("getLoadCaster", loadFunc.getLastMethodCalled());
        
        wrapper.prepareToRead(null,null);
        assertEquals("prepareToRead", loadFunc.getLastMethodCalled());
        
        wrapper.getNext();
        assertEquals("getNext", loadFunc.getLastMethodCalled());
        
        wrapper.setUDFContextSignature(null);
        assertEquals("setUDFContextSignature", loadFunc.getLastMethodCalled());

        NoSuchElementException e = null;
        try {
            loadFunc.getLastMethodCalled();
        } catch (NoSuchElementException ex) {
            e = ex;
        }

        assertNotNull("The wrapped class had more method invokations than it should have", e);
    }

    // Asserts that each call to an improperly initialized Wrapper fails.
    @Test
    public void testError() throws IOException {
        MockWrapper wrapper = new MockWrapper(); // <-- !! loadFunc not set !!

        IllegalArgumentException e = null;
        try {
            wrapper.getSchema(null, null);
        } catch (IllegalArgumentException ex) {
            e = ex;
        }

        assertNotNull("A useful exception should have been thrown when a method is called on an "
            + "improperly initialized LoadFuncMetadataWrapper", e);
        assertTrue("The method name that caused the problem should have been mentioned in the "
            + "exception, since the stack trace gets swallowed when this occurs",
            e.getMessage().contains("getSchema"));
    }

    private static class MockWrapper extends LoadFuncMetadataWrapper {
        private MockWrapper() { }

        private MockWrapper(LoadMetadata loadMetadata) {
            setLoadFunc(loadMetadata);
        }
    }

    private static class DummyLoadMetadata extends TestLoadFuncWrapper.DummyLoadFunc
                                          implements LoadMetadata {
        @Override
        public ResourceSchema getSchema(String location, Job job) throws IOException {
            setLastMethodCalled();
            return null;
        }

        @Override
        public ResourceStatistics getStatistics(String location, Job job) throws IOException {
            setLastMethodCalled();
            return null;
        }

        @Override
        public String[] getPartitionKeys(String location, Job job) throws IOException {
            setLastMethodCalled();
            return null;
        }

        @Override
        public void setPartitionFilter(Expression partitionFilter) throws IOException {
            setLastMethodCalled();
        }
    }
}
