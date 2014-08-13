/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.piggybank.storage.allloader.LoadFuncHelper;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Tests that the LoadFunchelper's public method returns the values required.
 */
public class TestLoadFuncHelper extends TestCase {

    private static final String LOADER = "org.apache.pig.builtin.PigStorage()";
    private static final String extensionLoaders = "txt:" + LOADER;
    Configuration configuration;
    LoadFuncHelper helper = null;

    File baseDir;

    File testFile;

    @Test
    public void testDetermineFunctionSingleArg() throws IOException {

        FuncSpec funcSpec = helper.determineFunction(baseDir.getAbsolutePath());

        assertNotNull(funcSpec);
        assertEquals(LOADER, funcSpec.toString() + "()");

    }

    @Test
    public void testDetermineFunction() throws IOException {

        Path firstFile = helper.determineFirstFile(baseDir.getAbsolutePath());
        FuncSpec funcSpec = helper.determineFunction(baseDir.getAbsolutePath(),
                firstFile);

        assertNotNull(funcSpec);
        assertEquals(LOADER, funcSpec.toString() + "()");

    }

    @Test
    public void testDetermineFirstFile() throws IOException {

        Path path = helper.determineFirstFile(baseDir.getAbsolutePath());

        assertNotNull(path);
        assertEquals(new Path(testFile.getAbsolutePath()).toUri().toString(), path.toUri().toURL().getFile());

    }

    @Before
    public void setUp() throws Exception {

        if (baseDir == null) {

            // we need this here for some strange reason while running ant test
            // the FileSystem.get call in the LoadFuncHelper will try and
            // connect to localhost??
            PigServer pig = new PigServer(ExecType.LOCAL);

            configuration = new Configuration(false);

            configuration.set(LoadFuncHelper.FILE_EXTENSION_LOADERS,
                    extensionLoaders);

            helper = new LoadFuncHelper(configuration);

            baseDir = new File("build/test/testLoadFuncHelper");
            if (baseDir.exists()) {
                FileUtil.fullyDelete(baseDir);
            }

            assertTrue(baseDir.mkdirs());

            testFile = new File(baseDir, "testFile.txt");
            FileWriter writer = new FileWriter(testFile);
            try {
                for (int i = 0; i < 100; i++) {
                    writer.append("test test\n");
                }
            } finally {
                writer.close();
            }

        }
    }

    @Override
    protected void tearDown() throws Exception {
        FileUtil.fullyDelete(baseDir);
        baseDir = null;
    }

}
