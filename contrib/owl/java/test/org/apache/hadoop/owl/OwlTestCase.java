
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

package org.apache.hadoop.owl;

import java.io.PrintStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class OwlTestCase extends TestCase {

    private static boolean readyForTest = false;

    // Used for time metrics for tests
    private static long timeStarted = 0;
    private static long timeStopped = 0;

    protected static void startStopWatch(){
        timeStarted = System.currentTimeMillis();
    }

    protected static void stopStopWatch(){
        timeStopped = System.currentTimeMillis();
    }

    protected static long timeElapsed() throws Exception{
        if ((timeStarted == 0) || (timeStopped < timeStarted)){
            throw new Exception("Stopwatch not started or started and not stopped.");
        }
        return (timeStopped - timeStarted);
    }


    @Before
    public void setUp() {

        LogHandler.configureDefaultAppender();
        LogHandler.setLogLevel("client", "DEBUG");
        readyForTest = true;
        startStopWatch();
    }

    @After
    public void tearDown(){
        stopStopWatch();
        LogHandler.resetLogConfiguration();
        readyForTest = false;
        try {
            System.out.println("Test [" + getName() + "] took [" + timeElapsed() + "] milliseconds");
        } catch (Exception e) {
            e.printStackTrace();
            assertNull(e);
        }
    }

    @SuppressWarnings("unused")
    private static PrintStream savedStdout = System.out;


    protected boolean checkSetup(){
        if (!readyForTest){
            // setup hasn't run yet, run it.
            setUp();
            Log log = LogFactory.getLog("client");
            log.warn("test setUp() hadn't already run, doing so explicitly");
        }
        return readyForTest;
    }

    protected static String getRandomString() {
        return Double.toString(Math.random());
    }

    protected static String dumpMDOs(OwlObject m1, OwlObject m2) throws OwlException {
        return "[" + OwlUtil.getJSONFromObject(m1) + "]vs[" + OwlUtil.getJSONFromObject(m2) + "]";
    }



    private static final String URI_LOCALHOST = "http://localhost:8078/owl/rest";
    private static final String OWL_URI = URI_LOCALHOST;

    // Note: URL not set here now - set in build.xml as a system property (org.apache.hadoop.owl.test.uri)
    // These definitions above are used only if it isn't set there.

    private static final String PROPERTY_OWL_UNIT_TEST_URI = "org.apache.hadoop.owl.test.uri";

    public static String getUri(){
        String uri = System.getProperty(PROPERTY_OWL_UNIT_TEST_URI);
        if ((uri == null)||(uri.length() == 0)){
            return OWL_URI;
        }else{
            return uri;
        }
    }

    @Test
    public static void testNothing(){ 
        System.out.println("Hi! This particular test is not a test - please move along to the next. :)");
        assertTrue(true);
    }
}

