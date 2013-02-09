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

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.pig.test.TestPigRunner.TestNotificationListener;
import org.junit.Test;

public class TestMain {

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

    public static class TestNotificationListener2 extends TestNotificationListener {
        protected boolean hadArgs = false;
        public TestNotificationListener2() {}
        public TestNotificationListener2(String s) {
            hadArgs = true;
        }
    }

}
