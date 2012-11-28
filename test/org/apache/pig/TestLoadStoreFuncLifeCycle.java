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
package org.apache.pig;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestLoadStoreFuncLifeCycle {

    public static String loaderSignature;
    public static String storerSignature;

    abstract public static class InstrumentedStorage extends Storage implements LoadPushDown {

        protected final int id;

        abstract void incCall();

        private void logCaller(Object... params) {
            incCall();
            TestLoadStoreFuncLifeCycle.logCaller(id, this.getClass(), params);
        }

        public InstrumentedStorage(int id) {
            super();
            this.id = id;
            logCaller();
        }

        @Override
        public String relativeToAbsolutePath(String location, Path curDir)
                throws IOException {
            logCaller(location, curDir);
            return super.relativeToAbsolutePath(location, curDir);
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
            logCaller(location, job);
            super.setLocation(location, job);
        }

        @Override
        public InputFormat<?, ?> getInputFormat() throws IOException {
            logCaller();
            return super.getInputFormat();
        }

        @Override
        public LoadCaster getLoadCaster() throws IOException {
            logCaller();
            return super.getLoadCaster();
        }

        @Override
        public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
                throws IOException {
            logCaller(reader, split);
            super.prepareToRead(reader, split);
        }

        @Override
        public Tuple getNext() throws IOException {
            logCaller();
            return super.getNext();
        }

        @Override
        public void setUDFContextSignature(String signature) {
            logCaller(signature);
            if (TestLoadStoreFuncLifeCycle.loaderSignature == null) {
                TestLoadStoreFuncLifeCycle.loaderSignature = signature;
            } else {
                assertEquals(TestLoadStoreFuncLifeCycle.loaderSignature, signature);
            }
            super.setUDFContextSignature(signature);
        }

        @Override
        public ResourceSchema getSchema(String location, Job job)
                throws IOException {
            logCaller(location, job);
            return super.getSchema(location, job);
        }

        @Override
        public ResourceStatistics getStatistics(String location, Job job)
                throws IOException {
            logCaller(location, job);
            return super.getStatistics(location, job);
        }

        @Override
        public String[] getPartitionKeys(String location, Job job)
                throws IOException {
            logCaller(location, job);
            return super.getPartitionKeys(location, job);
        }

        @Override
        public void setPartitionFilter(Expression partitionFilter)
                throws IOException {
            logCaller(partitionFilter);
            super.setPartitionFilter(partitionFilter);
        }

        @Override
        public List<OperatorSet> getFeatures() {
            logCaller();
            return null;
        }

        @Override
        public RequiredFieldResponse pushProjection(
                RequiredFieldList requiredFieldList) throws FrontendException {
            logCaller(requiredFieldList);
            return null;
        }

        @Override
        public String relToAbsPathForStoreLocation(String location, Path curDir)
                throws IOException {
            logCaller(location, curDir);
            return super.relToAbsPathForStoreLocation(location, curDir);
        }

        @Override
        public OutputFormat<?, ?> getOutputFormat() throws IOException {
            logCaller();
            return super.getOutputFormat();
        }

        @Override
        public void setStoreLocation(String location, Job job) throws IOException {
            logCaller(location, job);
            super.setStoreLocation(location, job);
        }

        @Override
        public void checkSchema(ResourceSchema s) throws IOException {
            logCaller(s);
            super.checkSchema(s);
        }

        @Override
        public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
            logCaller(writer);
            super.prepareToWrite(writer);
        }

        @Override
        public void putNext(Tuple t) throws IOException {
            logCaller(t);
            super.putNext(t);
        }

        @Override
        public void setStoreFuncUDFContextSignature(String signature) {
            logCaller(signature);
            if (TestLoadStoreFuncLifeCycle.storerSignature == null) {
                TestLoadStoreFuncLifeCycle.storerSignature = signature;
            } else {
                assertEquals(TestLoadStoreFuncLifeCycle.storerSignature, signature);
            }
            super.setStoreFuncUDFContextSignature(signature);
        }

        @Override
        public void cleanupOnFailure(String location, Job job) throws IOException {
            logCaller(location, job);
            super.cleanupOnFailure(location, job);
        }

        @Override
        public void storeStatistics(ResourceStatistics stats, String location,
                Job job) throws IOException {
            logCaller(stats, location, job);
            super.storeStatistics(stats, location, job);
        }

        @Override
        public void storeSchema(ResourceSchema schema, String location, Job job)
                throws IOException {
            logCaller(schema, location, job);
            super.storeSchema(schema, location, job);
        }

    }

    public static class Loader extends InstrumentedStorage {
        static int count;
        static int callCount;

        public Loader() {
            super(++ count);
        }

        @Override
        void incCall() {
            ++callCount;
        }

    }

    public static class Storer extends InstrumentedStorage {
        static int count;
        static int callCount;

        public Storer() {
            super(++ count);
        }

        @Override
        void incCall() {
            ++callCount;
        }

    }

    private static final int MAX_PARAM_SIZE = 70;

    static List<String> calls = new ArrayList<String>();
    static List<String> constructorCallers = new ArrayList<String>();

    private static void logCaller(int id, Class<?> clazz, Object[] params) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int i = 0;
        // skip the stackframes that where after the call to logCaller (included)
        while (!stackTrace[i].toString().startsWith(InstrumentedStorage.class.getName() + ".logCaller")) {
            ++i;
        }
        StackTraceElement called = stackTrace[i + 1];
        String calledClass = clazz.getSimpleName();

        String paramsString = null;
        for (Object param : params) {
            String paramString = null;
            if (param instanceof Job) {
                paramString = ((Job)param).getJobName();
            } else {
                paramString = String.valueOf(param);
            }
            if (paramString.length() > MAX_PARAM_SIZE || paramString.contains("\n")) {
                int end = paramString.indexOf('\n');
                if (end == -1 || end > MAX_PARAM_SIZE) {
                    end = MAX_PARAM_SIZE;
                }
                paramString = paramString.substring(0, end) + "...";
            }
            if (paramsString == null) {
                paramsString = "(";
            } else {
                paramsString += ", ";
            }
            paramsString += paramString;
        }
        if (paramsString == null) {
            paramsString = "()";
        } else {
            paramsString += ")";
        }
        String call = calledClass + "[" + id + "]." + called.getMethodName();
        calls.add(call + paramsString);
        if (called.getMethodName().equals("<init>")) {
            constructorCallers.add(call + " called by " + findSalient(stackTrace));
        }
    }

    /**
     * helper method for debugging
     * @param stackTrace the stack trace of the constructor call
     * @return the first interesting stack frame of where the constructor was called from
     */
    private static String findSalient(StackTraceElement[] stackTrace) {
        String message = "";
        int count = 0;
        // when finding the caller we want to keep only the pig calls to shorten the report
        for (StackTraceElement el : stackTrace) {
            String cl = el.getClassName();
            if (cl.startsWith("org.apache.pig")
                    && !cl.startsWith(TestLoadStoreFuncLifeCycle.class.getName())
                    ) {
                message += "\n" + el.toString();
                ++ count;
            }
            if (count > 15) {
                break;
            }
        }

        return message;
    }

    @Test
    public void testLoadStoreFunc() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        Data data = Storage.resetData(pigServer.getPigContext());
        data.set("foo",
                tuple("a"),
                tuple("b"),
                tuple("c")
                );

        pigServer.registerQuery(
                "A = LOAD 'foo' USING " + TestLoadStoreFuncLifeCycle.class.getName() + "$Loader();\n" +
                        "STORE A INTO 'bar' USING " + TestLoadStoreFuncLifeCycle.class.getName() + "$Storer();");

        List<Tuple> out = data.get("bar");

        assertEquals("a", out.get(0).get(0));
        assertEquals("b", out.get(1).get(0));
        assertEquals("c", out.get(2).get(0));

        assertTrue("loader instanciation count increasing: " + Loader.count, Loader.count <= 3);
        // LocalJobRunner gets the outputcommitter to call setupJob in Hadoop
        // 2.0.x which was not done in Hadoop 1.0.x. (MAPREDUCE-3563) As a
        // result, the number of StoreFunc instances is greater by 1 in
        // Hadoop-2.0.x.
        assertTrue("storer instanciation count increasing: " + Storer.count,
                Storer.count <= (Util.isHadoop2_0() ? 5 : 4));

    }

    /**
     * prints out a report on instantiation sites and method calls
     */
    public static void main(String[] args) throws Exception {
        new TestLoadStoreFuncLifeCycle().testLoadStoreFunc();
        System.out.println("report:");
        System.out.println(Loader.count + " instances of Loader");
        System.out.println(Loader.callCount + " calls to Loader");
        System.out.println(Storer.count + " instances of Storer");
        System.out.println(Storer.callCount + " calls to Storer");
        System.out.println();
        System.out.println("all calls:");
        for (String caller : calls) {
            System.out.println(caller);
        }
        System.out.println();
        System.out.println("constructor calls:");
        for (String caller : constructorCallers) {
            System.out.println(caller);
        }
    }

}
