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

package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.junit.Test;

/**
 * Test cases for FuncSpec class
 */
public class TestFuncSpec {

    @Test
    public void testSpecCtorClassName() {
        String pigStorage = PigStorage.class.getName();
        FuncSpec fs = new FuncSpec(pigStorage);
        Object o = PigContext.instantiateFuncFromSpec(fs);
        assertTrue(o instanceof PigStorage);
    }

    @Test
    public void testSpecCtorClassNameNoArgs() {
        String pigStorage = PigStorage.class.getName();
        FuncSpec fs = new FuncSpec(pigStorage+"()");
        Object o = PigContext.instantiateFuncFromSpec(fs);
        assertTrue(o instanceof PigStorage);
    }

    @Test
    public void testSpecCtorClassNameWithArgs() {
        String dummy = DummyClass.class.getName();
        FuncSpec fs = new FuncSpec(dummy+"(':')");
        Object o = PigContext.instantiateFuncFromSpec(fs);
        assertTrue(o instanceof DummyClass);
        assertEquals((byte)':', ((DummyClass)o).delim);
    }

    @Test
    public void testCtorClassNameArgs() {
        String dummy = DummyClass.class.getName();
        String[] args = new String[]{":"};
        FuncSpec fs = new FuncSpec(dummy, args);
        Object o = PigContext.instantiateFuncFromSpec(fs);
        assertTrue(o instanceof DummyClass);
        assertEquals((byte)':', ((DummyClass)o).delim);
    }

    public static class DummyClass {

        public byte delim = '\t';

        public DummyClass(String delim) {
            this.delim = (byte)(delim.charAt(0));
        }
    }
}