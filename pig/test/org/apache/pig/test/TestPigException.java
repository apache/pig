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

import junit.framework.Assert;
import org.apache.pig.PigException;
import org.apache.pig.impl.util.LogUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestPigException {

    private static final int NONZERO_ERRORCODE = 1;

    @Test
    public void testNestedException() {
        PigException innermost = new PigException("innermost", 
                NONZERO_ERRORCODE);
        PigException inner = new PigException("inner", 
                NONZERO_ERRORCODE, innermost);
        PigException outer = new PigException("outer", 
                NONZERO_ERRORCODE, inner);
        Assert.assertEquals(innermost.getMessage(),
                LogUtils.getPigException(outer).getMessage());
    }

    @Test
    public void testStickyNestedException() {
        PigException innermost = new PigException("innermost", 
                NONZERO_ERRORCODE);
        PigException inner = new PigException("inner", 
                NONZERO_ERRORCODE, innermost);
        inner.setMarkedAsShowToUser(true);
        PigException outer = new PigException("outer", 
                NONZERO_ERRORCODE, inner);
        Assert.assertEquals(inner.getMessage(),
                LogUtils.getPigException(outer).getMessage());
    }
}
