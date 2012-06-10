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
package org.apache.pig.data.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.pig.data.utils.HierarchyHelper.MustOverride;
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
