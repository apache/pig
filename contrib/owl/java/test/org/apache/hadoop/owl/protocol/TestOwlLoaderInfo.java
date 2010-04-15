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

package org.apache.hadoop.owl.protocol;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.junit.Test;

public class TestOwlLoaderInfo extends OwlTestCase {


    final static String DRIVER_CLASS = "blahLoader.class";

    @Test
    public static void testOwlLoaderInfoSimpleConstructor() throws OwlException{
        OwlLoaderInfo o1 = new OwlLoaderInfo(DRIVER_CLASS);
        assertTrue(o1 != null);
        assertTrue(o1.getInputDriverClass().equals(DRIVER_CLASS));
        assertTrue(o1.getInputDriverArgs() == null);

        String serializedForm = o1.toString();
        assertTrue(serializedForm != null);
        assertTrue(serializedForm.length() > 0);

        OwlLoaderInfo o2 = new OwlLoaderInfo(serializedForm);
        assertTrue(o1.equals(o2));
        assertTrue(o2.getInputDriverClass().equals(DRIVER_CLASS));
        assertTrue(o2.getInputDriverArgs() == null);

    }

    private static void _doCtorTest(String driverClass, String driverArgs) throws OwlException {
        OwlLoaderInfo o1 = new OwlLoaderInfo(driverClass,driverArgs);
        assertTrue(o1 != null);
        assertTrue(o1.getInputDriverClass() != null);
        assertTrue(o1.getInputDriverClass().equals(driverClass));
        assertTrue(o1.getInputDriverArgs() != null);
        assertTrue(o1.getInputDriverArgs().equals(driverArgs));

        String serializedForm = o1.toString();
        assertTrue(serializedForm != null);
        assertTrue(serializedForm.length() > 0);

        OwlLoaderInfo o2 = new OwlLoaderInfo(serializedForm);
        assertTrue(o1.equals(o2));

        OwlLoaderInfo o3 = new OwlLoaderInfo(o2);
        assertTrue(o1.equals(o3));
    }

    @Test
    public static void testOwlLoaderInfoArgumentsConstructor() throws OwlException{

        _doCtorTest(DRIVER_CLASS, "uno");
        _doCtorTest(DRIVER_CLASS, ",");
        _doCtorTest(DRIVER_CLASS, "\001");
        _doCtorTest(DRIVER_CLASS,"");
        _doCtorTest(DRIVER_CLASS, "lorem,ipsum,dolor,sit,amet");
        _doCtorTest(DRIVER_CLASS,"\t");
    }

}
