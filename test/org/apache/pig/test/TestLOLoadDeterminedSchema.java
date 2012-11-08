/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.ScriptSchemaTestLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * Tests that the LOLoad class sets the script schema correctly as expected.<br/>
 * This way of passing the script schema is not an optimal solution but will be
 * used currently inorder not to break code by adding new methods the to
 * LoadFunc or other Classes. For more information please see
 * https://issues.apache.org/jira/browse/PIG-1717
 *
 */
public class TestLOLoadDeterminedSchema {
    PigContext pc;
    PigServer server;

    File baseDir;
    File inputFile;

    /**
     * Loads a test file using ScriptSchemaTestLoader with a user defined schema
     * a,b,c.<br/>
     * Then tests the the ScriptSchemaTestLoader found the schema.
     *
     * @throws IOException
     */
    @Test
    public void testDeterminedSchema() throws IOException {
        FuncSpec funcSpec = new FuncSpec(ScriptSchemaTestLoader.class.getName()
                + "()");

        server.registerFunction(ScriptSchemaTestLoader.class.getName(),
                funcSpec);

        server.registerQuery("a = LOAD '" + Util.encodeEscape(inputFile.getAbsolutePath())
                + "' using " + ScriptSchemaTestLoader.class.getName()
                + "() as (a, b, c) ;");

        server.openIterator("a");

        Schema scriptSchema = ScriptSchemaTestLoader.getScriptSchema();

        assertNotNull(scriptSchema);
        assertEquals(3, scriptSchema.size());

        assertNotNull(scriptSchema.getField("a"));
        assertNotNull(scriptSchema.getField("b"));
        assertNotNull(scriptSchema.getField("c"));
    }

    @Before
    public void setUp() throws Exception {
        FileLocalizer.deleteTempFiles();
        server = new PigServer(ExecType.LOCAL, new Properties());

        baseDir = new File("build/testLoLoadDeterminedSchema");

        if (baseDir.exists()) {
            FileUtil.fullyDelete(baseDir);
        }

        assertTrue(baseDir.mkdirs());

        inputFile = new File(baseDir, "testInput.txt");
        inputFile.createNewFile();

        // write a short input
        FileWriter writer = new FileWriter(inputFile);
        try {
            writer.write("a\tb\tc");
        } finally {
            writer.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (baseDir.exists())
            FileUtil.fullyDelete(baseDir);

        server.shutdown();
    }
}

