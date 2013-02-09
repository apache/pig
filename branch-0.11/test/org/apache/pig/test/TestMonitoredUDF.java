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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.MonitoredUDFExecutor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.MonitoredUDFExecutor.ErrorCallback;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestMonitoredUDF {

    @Test
    public void testTimeout() throws IOException {

        SimpleUDF udf = new SimpleUDF(1000);
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(udf);
        assertNull(exec.monitorExec(null));
    }

    @Test
    public void testTimeoutWithDefault() throws IOException {

        SimpleIntUDF udf = new SimpleIntUDF();
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(udf);
        assertEquals( SimpleIntUDF.DEFAULT, ((Integer) exec.monitorExec(null)).intValue());
    }

    @Test
    public void testInheritance() throws IOException {
        InheritedUdf udf = new InheritedUdf();
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(udf);
        assertEquals( SimpleIntUDF.DEFAULT, ((Integer) exec.monitorExec(null)).intValue());
    }

    @Test
    public void testCustomErrorHandler() throws IOException {

        ErrorCallbackUDF udf = new ErrorCallbackUDF();
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(udf);
        exec.monitorExec(null);
        assertTrue(thereWasATimeout);
    }

    @Test
    public void testNoTimeout() throws IOException {
        SimpleUDF udf = new SimpleUDF(100);
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(udf);
        assertTrue((Boolean) exec.monitorExec(null));
    }

    /**
     * This main method runs a microbenchmark for measuring overhead caused by
     * annotating a UDF as monitored.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        long unmonitoredTime = 0, monitoredTime = 0;
        
        int[] numReps = { 1000, 10000, 100000, 1000000};
        MonitoredNoOpUDF monitoredUdf  = new MonitoredNoOpUDF();
        MonitoredUDFExecutor exec = new MonitoredUDFExecutor(monitoredUdf);
        UnmonitoredNoOpUDF unmonitoredUdf = new UnmonitoredNoOpUDF();
        // warm up
        System.out.println("Warming up.");
        for (int i : numReps) {
            for (int j=0; j < i; j++) {
                exec.monitorExec(null);
                unmonitoredUdf.exec(null);
            }
        }
        System.out.println("Warmed up. Timing.");
        // tests!
        for (int k = 0; k < 5; k++) {
            for (int i : numReps) {
                startTime = System.currentTimeMillis();
                for (int j = 0; j < i; j++) {
                    exec.monitorExec(null);
                }
                monitoredTime = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                for (int j = 0; j < i; j++) {
                    unmonitoredUdf.exec(null);
                }
                unmonitoredTime = System.currentTimeMillis() - startTime;
                System.out.println("Reps: " + i + " monitored: " + monitoredTime + " unmonitored: " + unmonitoredTime);
            }    
        }
    }

    @MonitoredUDF(timeUnit = TimeUnit.MILLISECONDS, duration = 500)
    public class SimpleUDF extends EvalFunc<Boolean> {
        int wait;
        public SimpleUDF(int wait) {
            this.wait = wait;
        }

        @Override
        public Boolean exec(Tuple input) throws IOException {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return true;
        }
    }
    
    public static class UnmonitoredNoOpUDF extends EvalFunc<Boolean> {
        @Override public Boolean exec(Tuple input) throws IOException { 
            System.currentTimeMillis(); return true; }
    }

    @MonitoredUDF(timeUnit = TimeUnit.MILLISECONDS, duration = 500)
    public static class MonitoredNoOpUDF extends EvalFunc<Boolean> {
        @Override public Boolean exec(Tuple input) throws IOException { 
            System.currentTimeMillis(); return true; }
    }

    
    @MonitoredUDF(timeUnit = TimeUnit.MILLISECONDS, duration = 100, intDefault = SimpleIntUDF.DEFAULT)
    public static class SimpleIntUDF extends EvalFunc<Integer> {
        public static final int DEFAULT = 123;
        public static final int NOT_DEFAULT = 321;

        @Override
        public Integer exec(Tuple input) throws IOException {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return  NOT_DEFAULT;
        }
    }

    public static class InheritedUdf extends SimpleIntUDF {}

    @MonitoredUDF(timeUnit = TimeUnit.MILLISECONDS, duration = 100, errorCallback = CustomErrorCallback.class)
    public class ErrorCallbackUDF extends EvalFunc<Integer> {

        @Override
        public Integer exec(Tuple input) throws IOException {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return null;
        }

    }

    static boolean thereWasATimeout = false;

    public static class CustomErrorCallback extends ErrorCallback {

        @SuppressWarnings("unchecked")
        public static void handleTimeout(EvalFunc evalFunc, Exception e) {
            thereWasATimeout = true;
        }
    }
}
