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

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.junit.Test;

/**
 * Tests that the LoadFuncWrapper properly wraps LoadFunc objects
 */
public class TestLoadFuncWrapper {

    // Asserts that each call to a Wrapper invokes the correct method on the Wrappee.
    @Test
    public void testSucess() throws IOException {
        DummyLoadFunc loadFunc = new DummyLoadFunc();
        MockWrapper wrapper = new MockWrapper(loadFunc);

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
            wrapper.getInputFormat();
        } catch (IllegalArgumentException ex) {
            e = ex;
        }

        assertNotNull("A useful exception should have been thrown when a method is called on an "
            + "improperly initialized LoadFuncWrapper", e);
        assertTrue("The method name that caused the problem should have been mentioned in the "
            + "exception, since the stack trace gets swallowed when this occurs",
            e.getMessage().contains("getInputFormat"));
    }

    private static class MockWrapper extends LoadFuncWrapper {
        private MockWrapper() { }

        private MockWrapper(LoadFunc loadFunc) {
            setLoadFunc(loadFunc);
        }
    }

    public static class DummyLoadFunc extends LoadFunc {
        private LinkedList<String> methodCalls = new LinkedList<String>();

        public String getLastMethodCalled() { return methodCalls.pop(); }
        protected void setLastMethodCalled() {
            methodCalls.push(Thread.currentThread().getStackTrace()[2].getMethodName());
        }
        
        @Override
        public String relativeToAbsolutePath(String location, Path curDir) {
            setLastMethodCalled();
            return null;
        }
        
        @Override
        public void setLocation(String location, Job job) throws IOException {
            setLastMethodCalled();
        }
        @Override
        public InputFormat getInputFormat() throws IOException {
            setLastMethodCalled();
            return null;
        }
        
        @Override
        public LoadCaster getLoadCaster() throws IOException {
            setLastMethodCalled();
            return null;
        }
        
        @Override
        public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
            setLastMethodCalled();            
        }
        
        @Override
        public Tuple getNext() throws IOException {
            setLastMethodCalled();
            return null;
        }
        
        @Override
        public void setUDFContextSignature(String signature) {
            setLastMethodCalled();
        }
    
    }
}
