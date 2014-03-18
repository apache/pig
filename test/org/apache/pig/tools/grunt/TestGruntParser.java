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
package org.apache.pig.tools.grunt;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser.ExplainState;
import org.junit.Before;
import org.junit.Test;

public class TestGruntParser {
    private GruntParser parser;
    static PigServer pig;
    
    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testProcessRemove() throws IOException {
        File tmpFile = File.createTempFile("TestGruntParser", "testProcessRemove");
        String dummyScript = "";
        parser = new GruntParser(new ByteArrayInputStream(dummyScript.getBytes()), pig);

        //Delete existing file and check that it doesn't exist
        parser.processRemove(tmpFile.getAbsolutePath(), "");
        assertFalse(tmpFile.exists());

        //Delete non-existing file and check for exception
        try {
            parser.processRemove(tmpFile.getAbsolutePath(), "");
            fail("processRemove should throw exception when deleting missing file.");
        } catch (IOException e) {
            //Yay
        }
        
        //Delete non-existing file with force, no exception
        parser.processRemove(tmpFile.getAbsolutePath(), "force");
        
        //Ensure that remove isn't processed when in explain mode
        tmpFile = File.createTempFile("TestGruntParser", "testProcessRemove");
        parser.setExplainState(new ExplainState("","","",false,""));
        parser.processRemove(tmpFile.getAbsolutePath(), "force");
        assertTrue(tmpFile.exists());
    }
}
