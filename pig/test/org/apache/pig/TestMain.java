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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.SourceLocation;
import org.apache.pig.test.TestPigRunner.TestNotificationListener;
import org.apache.pig.tools.parameters.ParameterSubstitutionException;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.Test;

public class TestMain {
    private Log log = LogFactory.getLog(TestMain.class);

    @Test
    public void testCustomListener() {
        Properties p = new Properties();
        p.setProperty(Main.PROGRESS_NOTIFICATION_LISTENER_KEY, TestNotificationListener2.class.getName());
        TestNotificationListener2 listener = (TestNotificationListener2) Main.makeListener(p);
        assertEquals(TestNotificationListener2.class, Main.makeListener(p).getClass());
        assertFalse(listener.hadArgs);

        p.setProperty(Main.PROGRESS_NOTIFICATION_LISTENER_ARG_KEY, "foo");
        listener = (TestNotificationListener2) Main.makeListener(p);
        assertEquals(TestNotificationListener2.class, Main.makeListener(p).getClass());
        assertTrue(listener.hadArgs);

    }
    
    @Test
    public void testRun_setsErrorThrowableOnPigStats() {
        File outputFile = null;
        try {
            String filename = this.getClass().getSimpleName() + "_" + "testRun_setsErrorThrowableOnPigStats";
            outputFile = File.createTempFile(filename, ".out");
            outputFile.delete();
            
            File scriptFile = File.createTempFile(filename, ".pig");
            BufferedWriter bw = new BufferedWriter(new FileWriter(scriptFile));
            bw.write("a = load 'test/org/apache/pig/test/data/passwd';\n");
            bw.write("b = group a by $0\n");
            bw.write("c = foreach b generate group, COUNT(a) as cnt;\n");
            bw.write("store c into 'out'\n");
            bw.close();
            
            Main.run(new String[]{"-x", "local", scriptFile.getAbsolutePath()}, null);
            PigStats stats = PigStats.get();
            
            Throwable t = stats.getErrorThrowable();
            assertTrue(t instanceof FrontendException);
            
            FrontendException fe = (FrontendException) t;
            SourceLocation sl = fe.getSourceLocation();
            assertEquals(2, sl.line());
            assertEquals(15, sl.offset());
            
            Throwable cause = fe.getCause();
            assertTrue(cause instanceof ParserException);
            
        } catch (Exception e) {
            log.error("Encountered exception", e);
            fail("Encountered Exception");
        }
    }
    
    @Test
    public void testRun_setsErrorThrowableForParamSubstitution() {
        File outputFile = null;
        try {
            String filename = this.getClass().getSimpleName() + "_" + "testRun_setsErrorThrowableForParamSubstitution";
            outputFile = File.createTempFile(filename, ".out");
            outputFile.delete();

            File scriptFile = File.createTempFile(filename, ".pig");
            BufferedWriter bw = new BufferedWriter(new FileWriter(scriptFile));
            bw.write("a = load '$NOEXIST';\n");
            bw.write("b = group a by $0\n");
            bw.write("c = foreach b generate group, COUNT(a) as cnt;\n");
            bw.write("store c into 'out'\n");
            bw.close();
            
            Main.run(new String[]{"-x", "local", scriptFile.getAbsolutePath()}, null);
            PigStats stats = PigStats.get();
            
            Throwable t = stats.getErrorThrowable();
            assertTrue(t instanceof IOException);
            
            Throwable cause = t.getCause();
            assertTrue(cause instanceof ParameterSubstitutionException);

            ParameterSubstitutionException pse = (ParameterSubstitutionException) cause;
            assertTrue(pse.getMessage().contains("NOEXIST"));
        } catch (Exception e) {
            log.error("Encountered exception", e);
            fail("Encountered Exception");
        }
    }

    public static class TestNotificationListener2 extends TestNotificationListener {
        protected boolean hadArgs = false;
        public TestNotificationListener2() {}
        public TestNotificationListener2(String s) {
            hadArgs = true;
        }
    }

}
