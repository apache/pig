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

import static org.junit.Assert.*;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.UDFContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestUDFContext {
    
    static MiniCluster cluster = null;
    
    @Before
    public void setUp() throws Exception {
        cluster = MiniCluster.buildCluster();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testUDFContext() throws Exception {
        File a = Util.createLocalInputFile("a.txt", new String[] { "dumb" });
        File b = Util.createLocalInputFile("b.txt", new String[] { "dumber" });
        FileLocalizer.deleteTempFiles();
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        String[] statement = { "A = LOAD '" + a.getAbsolutePath() +
                "' USING org.apache.pig.test.utils.UDFContextTestLoader('joe');",
            "B = LOAD '" + b.getAbsolutePath() +
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
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery(" l = load 'file' as (a :int, b : int, c : int);");
        pig.registerQuery(" f = foreach l generate a, b;");        
        pig.explain("f", System.out);
        Properties props = UDFContext.getUDFContext().getUDFProperties(PigStorage.class);

        // required fields property should be set because f results does not
        // require the third column c, so properties should not be null
        assertNotNull(props.get("l"));
        
        // the new statement for alias f below will require all columns,
        // so this time required fields property for loader should not be set
        pig.registerQuery(" f = foreach l generate a, b, c;");
        pig.explain("f", System.out);
        props = UDFContext.getUDFContext().getUDFProperties(PigStorage.class);

        assertTrue("properties in udf context for load should be null", 
                props.get("l") == null);

        
    }
    
}
