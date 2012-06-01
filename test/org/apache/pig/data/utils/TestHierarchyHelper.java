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
        assertTrue("Test3 does not have a dangling MustOverride -- should be true"
            , HierarchyHelper.verifyMustOverride(Test3.class));
        assertFalse("Test4 has a dangling MustOverride -- should be false"
            , HierarchyHelper.verifyMustOverride(Test4.class));
        assertFalse("Test5 has a dangling MustOverride -- should be false"
            , HierarchyHelper.verifyMustOverride(Test5.class));
        assertFalse("Test6 has a dangling MustOverride -- should be false"
            , HierarchyHelper.verifyMustOverride(Test6.class));
    }

    static class Test1 {
        @MustOverride
        public void t1() {}
    }

    static class Test2 extends Test1 {
        public void t1() {}
    }

    static class Test3 extends Test2 {}

    static class Test4 extends Test1 {}

    static class Test5 extends Test4 {
        @MustOverride
        public void t1() {}

        @MustOverride
        public void t2() {}
    }

    static class Test6 extends Test5 {
        public void t1() {}
    }
}
