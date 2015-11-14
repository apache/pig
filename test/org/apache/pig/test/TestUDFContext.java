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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.junit.AfterClass;
import org.junit.Test;

public class TestUDFContext {

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        FileLocalizer.deleteTempFiles();
    }

    @Test
    public void testUDFContext() throws Exception {
        File a = Util.createInputFile("inp1", "txt", new String[] { "dumb" });
        File b = Util.createInputFile("inp2", "txt", new String[] { "dumber" });
        PigServer pig = new PigServer(Util.getLocalTestMode(), new Properties());
        String[] statement = { "A = LOAD '" + Util.encodeEscape(a.getAbsolutePath()) +
                "' USING org.apache.pig.test.utils.UDFContextTestLoader('joe');",
            "B = LOAD '" + Util.encodeEscape(b.getAbsolutePath()) +
            "' USING org.apache.pig.test.utils.UDFContextTestLoader('jane');",
            "C = union A, B;",
            "D = FOREACH C GENERATE $0, $1, org.apache.pig.test.utils.UDFContextTestEvalFunc($0), " +
            "org.apache.pig.test.utils.UDFContextTestEvalFunc2($0);" };

        File tmpFile = File.createTempFile("temp_jira_851", ".pig");
        FileWriter writer = new FileWriter(tmpFile);
        for (String line : statement) {
            writer.write(line + "\n");
        }
        writer.close();

        pig.registerScript(tmpFile.getAbsolutePath());
        Iterator<Tuple> iterator = pig.openIterator("D");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            if ("dumb".equals(tuple.get(0).toString())) {
                assertEquals(tuple.get(1).toString(), "joe");
            } else if ("dumber".equals(tuple.get(0).toString())) {
                assertEquals(tuple.get(1).toString(), "jane");
            }
        	assertEquals(Integer.valueOf(tuple.get(2).toString()), new Integer(5));
        	assertEquals(tuple.get(3).toString(), "five");
        }
    }


    /**
     * Test that UDFContext is reset each time the plan is regenerated
     * @throws Exception
     */
    @Test
    public void testUDFContextReset() throws Exception {
        PigServer pig = new PigServer(Util.getLocalTestMode());
        pig.registerQuery(" l = load 'file' as (a :int, b : int, c : int);");
        pig.registerQuery(" f = foreach l generate a, b;");
        pig.explain("f", System.out);
        Properties props = UDFContext.getUDFContext().getUDFProperties(PigStorage.class);

        // required fields property should be set because f results does not
        // require the third column c, so properties should not be null
        assertTrue(
                "properties in udf context for load should not be empty: "+props,
                props.keySet().size()>0);

        // the new statement for alias f below will require all columns,
        // so this time required fields property for loader should not be set
        pig.registerQuery(" f = foreach l generate a, b, c;");
        pig.explain("f", System.out);
        props = UDFContext.getUDFContext().getUDFProperties(PigStorage.class);

        assertTrue(
                "properties in udf context for load should be empty: "+props,
                props.keySet().size() == 0);


    }

    @Test
    public void testUDFContextSeparator() throws Exception {

        File inputFile = Util.createInputFile("input", "txt", new String[] { "f1\tf2\tf3\tf4\tf5" });

        PigServer pigServer = new PigServer(Util.getLocalTestMode(), new Properties());
        Storage.Data data = resetData(pigServer);

        String inputPath = Util.encodeEscape(inputFile.getAbsolutePath());
        String query = "A = LOAD '" + inputPath + "' USING PigStorage();"
                + "B = LOAD '" + inputPath + "' USING PigStorage();"
                + "B = FOREACH B GENERATE $0, $1;"
                + "C = LOAD '" + inputPath + "' USING " + FieldsByIndexLoader.class.getName() + "('1,2');"
                // Scalar to force PigStorage to be always visited first in LoaderProcessor
                + "C = FOREACH C GENERATE *, B.$0;"
                + "STORE A INTO 'A' USING mock.Storage();"
                + "STORE B INTO 'B' USING mock.Storage();"
                + "STORE C INTO 'C' USING mock.Storage();";

        pigServer.registerQuery(query);

        List<Tuple> a = data.get("A");
        List<Tuple> b = data.get("B");
        List<Tuple> c = data.get("C");
        assertEquals(1, a.size());
        assertEquals(1, b.size());
        assertEquals(1, c.size());
        DataByteArray f1 = new DataByteArray("f1");
        DataByteArray f2 = new DataByteArray("f2");
        DataByteArray f3 = new DataByteArray("f3");
        DataByteArray f4 = new DataByteArray("f4");
        DataByteArray f5 = new DataByteArray("f5");
        assertEquals(tuple(f1, f2, f3, f4, f5), a.get(0));
        assertEquals(tuple(f1, f2), b.get(0));
        assertEquals(tuple(f2, f3, f1), c.get(0));
    }


    public static class FieldsByIndexLoader extends PigStorage {

        // Tests the case of one user LoadFunc where UDF properties was a class variable
        // and getSchema() was used to determined frontend instead of
        // UDFContext.getUDFContext().isFrontEnd();
        private boolean frontend = false;
        private Properties props = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        private boolean[] selectedFields = new boolean[5]; //Assuming data always has 5 columns

        public FieldsByIndexLoader(String fieldIndices) {
            String[] requiredFields = fieldIndices.split(",");
            for (String index : requiredFields) {
                selectedFields[Integer.parseInt(index)] = true;
            }
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
            if (frontend) {
                // PigStorage should deserialize this as
                // mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
                props.setProperty(signature, ObjectSerializer.serialize(selectedFields));
            }
            super.setLocation(location, job);
        }

        @Override
        public ResourceSchema getSchema(String location, Job job)
                throws IOException {
            frontend = true;
            return super.getSchema(location, job);
        }

    }

}
