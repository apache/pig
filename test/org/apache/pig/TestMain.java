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
