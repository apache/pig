package org.apache.pig.data.utils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.pig.data.utils.HierarchyHelper.MustOverride;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestHierarchyHelper {
    @Test
    public void testOverride() {
        assertTrue("Test2 directly implements a MustOverride -- should be true"
            , HierarchyHelper.verifyMustOverride(Test2.class));
        assertFalse("Test3 does not directly implement a MustOverride (though it inherits one) -- should be false"
            , HierarchyHelper.verifyMustOverride(Test3.class));
    }

    static class Test1 {
        @MustOverride
        public void t1() {
        }
    }

    static class Test2 extends Test1 {
        public void t1() {
        }
    }

    static class Test3 extends Test2 {
    }
}
