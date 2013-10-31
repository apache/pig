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
import junit.framework.Assert;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.LineageFindRelVisitor;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLineageFindRelVisitor {

    public static class SillyLoadCasterWithExtraConstructor extends Utf8StorageConverter {
        public SillyLoadCasterWithExtraConstructor(String ignored) {
            super();
        }
    }
    public static class SillyLoaderWithLoadCasterWithExtraConstructor extends PigStorage {
        public static int counter=0;
        public SillyLoaderWithLoadCasterWithExtraConstructor() {
            super();
            counter++;
        }
        @Override
        public LoadCaster getLoadCaster() throws IOException {
            return new SillyLoadCasterWithExtraConstructor("abc");
        }
    }

    public static class SillyLoaderWithLoadCasterWithExtraConstructor2 extends PigStorage {
        public SillyLoaderWithLoadCasterWithExtraConstructor2() {
            super();
        }
        @Override
        public LoadCaster getLoadCaster() throws IOException {
            return new SillyLoadCasterWithExtraConstructor("abc");
        }
    }

    @Test
    public void testhaveIdenticalCasters() throws Exception {
        LogicalPlan lp = new LogicalPlan();
        Class<LineageFindRelVisitor> lineageFindRelVisitorClass = LineageFindRelVisitor.class;
        Method testMethod = lineageFindRelVisitorClass.getDeclaredMethod("haveIdenticalCasters",
                                                          FuncSpec.class,
                                                          FuncSpec.class);
        testMethod.setAccessible(true);

        LineageFindRelVisitor lineageFindRelVisitor = new LineageFindRelVisitor(lp);

        FuncSpec defaultSpec = new FuncSpec("PigStorage", new String [0]);

        // if either param is null, it should return false
        Assert.assertFalse("null param should always return false",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor, null, null) );
        Assert.assertFalse("null param should always return false",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor, null, defaultSpec) );
        Assert.assertFalse("null param should always return false",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor, defaultSpec, null ) );

        Assert.assertTrue("Same loaders are considered equal",
                          (Boolean) testMethod.invoke(lineageFindRelVisitor, defaultSpec, defaultSpec) );

        FuncSpec withDefaultCasterSpec = new FuncSpec("org.apache.pig.test.PigStorageWithStatistics", new String [0]);

        //PigStroage and PigStorageWithStatistics both use Utf8StorageConverter caster
        Assert.assertTrue("Different Loaders but same LoadCaster with only default constructor. They should be considered same.",
                          (Boolean) testMethod.invoke(lineageFindRelVisitor, defaultSpec, withDefaultCasterSpec) );


        FuncSpec casterWithExtraConstuctorSpec = new FuncSpec(
                  "org.apache.pig.test.TestLineageFindRelVisitor$SillyLoaderWithLoadCasterWithExtraConstructor",
                  new String[0]);
        FuncSpec casterWithExtraConstuctorSpec2 = new FuncSpec(
                  "org.apache.pig.test.TestLineageFindRelVisitor$SillyLoaderWithLoadCasterWithExtraConstructor2",
                  new String[0]);

        // these silly loaders have different classname but returns instance
        // from the same LoadCaster class.  However, this loadcaster takes extra param on
        // constructors and cannot guarantee equality

        Assert.assertFalse("Different Loader, different LoadCaster are definitely not equal",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor,
                                     defaultSpec, casterWithExtraConstuctorSpec) );

        Assert.assertFalse("Even when LoadCaster class matches, we consider them different when they have non-default constructor",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor,
                                     casterWithExtraConstuctorSpec, casterWithExtraConstuctorSpec2) );

        Assert.assertTrue("Same Loaders should be always equal irrespective of loadcaster constructors",
                           (Boolean) testMethod.invoke(lineageFindRelVisitor,
                                     casterWithExtraConstuctorSpec, casterWithExtraConstuctorSpec) );

        Assert.assertEquals("Loader should be instantiated at most once.", SillyLoaderWithLoadCasterWithExtraConstructor.counter, 1);
    }
}
