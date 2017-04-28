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
package org.apache.pig.tools.parameters;

import org.apache.hadoop.util.Shell;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.regex.Pattern;

public class TestPreprocessorContext {

    @Test
    public void testProcessShellCmd() throws ParameterSubstitutionException, FrontendException {
        PreprocessorContext ctx = new PreprocessorContext(0);
        String cmd = "echo hello";
        ctx.processShellCmd("some_value", "`" + cmd + "`");

        Map<String, String> paramVal = ctx.getParamVal();
        assertEquals("hello", paramVal.get("some_value"));
    }

    @Test
    public void testProcessShellCmdBigStderr() throws ParameterSubstitutionException, FrontendException {
        // This test probably doesn't work on Windows, but should work elsewhere
        Assume.assumeFalse(Shell.WINDOWS);

        PreprocessorContext ctx = new PreprocessorContext(0);
        String cmd = "bash -c 'i=0; while [ \"\\$i\" -lt 10000 ]; do echo long-stderr-output >&2; " +
                "i=\\$((i+1)); done; echo hello'";
        ctx.processShellCmd("some_value", "`" + cmd + "`");

        Map<String, String> paramVal = ctx.getParamVal();
        assertEquals("hello", paramVal.get("some_value"));
    }

    @Test
    public void testFailingCommand() throws ParameterSubstitutionException, FrontendException {
        try {
            PreprocessorContext ctx = new PreprocessorContext(0);
            String cmd = "exit 1";
            ctx.processShellCmd("some_value", "`" + cmd + "`");
        } catch (RuntimeException e) {
            assertTrue(Pattern.compile("Error executing shell command:.*exit code.*")
                    .matcher(e.getMessage()).matches()
            );
        }
    }
}
