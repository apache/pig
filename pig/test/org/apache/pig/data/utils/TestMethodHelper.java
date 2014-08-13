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

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;

import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.junit.Test;

public class TestMethodHelper {
    @Test
    public void testImplementation() throws NoSuchMethodException {
        Method t1 = ITest.class.getMethod("t1");
        Method t2 = ITest.class.getMethod("t2");

        shouldBe(t1, Test1.class, true);
        shouldBe(t2, Test1.class, true);

        shouldBe(t1, Test2.class, false);
        shouldBe(t2, Test2.class, true);

        shouldBe(t1, Test3.class, true);
        shouldBe(t2, Test3.class, true);

        shouldBe(t1, Test4.class, false);
        shouldBe(t2, Test4.class, true);

        shouldBe(t1, Test5.class, false);
        shouldBe(t2, Test5.class, false);

        shouldBe(t1, Test6.class, true);
        shouldBe(t2, Test6.class, false);

        shouldBe(t1, Test7.class, false);
        shouldBe(t2, Test7.class, false);
    }

    private void shouldBe(Method m, Class c, boolean b) {
        assertTrue(MethodHelper.isNotImplementedAnnotationPresent(m, c) == b);
    }

    static interface ITest {
        public void t1();

        public void t2();
    }

    static class Test1 implements ITest {
        @NotImplemented
        public void t1() {}

        @NotImplemented
        public void t2() {}
    }

    static class Test2 implements ITest {
        public void t1() {}

        @NotImplemented
        public void t2() {}
    }

    static class Test3 extends Test1 {}

    static class Test4 extends Test2 {}

    static class Test5 extends Test3 {
        public void t1() {}

        public void t2() {}
    }

    static class Test6 extends Test5 {
        @NotImplemented
        public void t1() {}
    }

    static class Test7 extends Test6 {
        public void t1() {}
    }

}
