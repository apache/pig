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
import static org.junit.Assert.fail;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.LineageFindRelVisitor;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLineageFindRelVisitor {

    private PigServer pig ;

    @Before
    public void setUp() throws Exception{
        pig = new PigServer(Util.getLocalTestMode()) ;
    }

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

    public static class ToTupleWithCustomLoadCaster extends org.apache.pig.builtin.TOTUPLE {
        @Override
        public LoadCaster getLoadCaster() throws IOException {
            return new SillyLoadCasterWithExtraConstructor("ignored");
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

        Assert.assertEquals("Loader should be instantiated at most once.", 1, SillyLoaderWithLoadCasterWithExtraConstructor.counter);
    }

    @Test
    public void testIdenticalColumnUDFForwardingLoadCaster() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
                Storage.tuple(Storage.map(
                                 "key1",new DataByteArray("aaa"),
                                 "key2",new DataByteArray("bbb"),
                                 "key3",new DataByteArray("ccc"))),
                Storage.tuple(Storage.map(
                                 "key1",new DataByteArray("zzz"),
                                 "key2",new DataByteArray("yyy"),
                                 "key3",new DataByteArray("xxx"))));
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (m:[bytearray]);");
        pig.registerQuery("B = foreach A GENERATE m#'key1' as key1, m#'key2' as key2; "
                // this equal comparison creates implicit typecast to chararray
                // which requires loadcaster
                + "C = FILTER B by key1 == 'aaa' and key2 == 'bbb';");
        pig.registerQuery("store C into 'output' using mock.Storage();");

        pig.executeBatch();

        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {"('aaa', 'bbb')"});
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testUDFForwardingLoadCaster() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
                Storage.tuple(new DataByteArray("aaa")),
                Storage.tuple(new DataByteArray("bbb")));
        pig.setBatchOn();
        String query = "A = load 'input' using mock.Storage() as (a1:bytearray);"
            + "B = foreach A GENERATE TOTUPLE(a1) as tupleA;"
            + "C = foreach B GENERATE (chararray) tupleA.a1;"  //using loadcaster
            + "store C into 'output' using mock.Storage();";

        LogicalPlan lp = Util.parse(query, pig.getPigContext());
        Util.optimizeNewLP(lp);

        CastFinder cf = new CastFinder(lp);
        cf.visit();
        Assert.assertEquals("There should be only one typecast expression.", 1, cf.casts.size());
        Assert.assertEquals("Loadcaster should be coming from the Load", "mock.Storage", cf.casts.get(0).getFuncSpec().getClassName());

        pig.registerQuery(query);
        pig.executeBatch();

        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {"('aaa')", "('bbb')"});
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testUDFgetLoadCaster() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
                Storage.tuple(new DataByteArray("aaa")),
                Storage.tuple(new DataByteArray("bbb")));
        pig.setBatchOn();
        String query = "A = load 'input' using mock.Storage() as (a1:bytearray);"
            + "B = foreach A GENERATE org.apache.pig.test.TestLineageFindRelVisitor$ToTupleWithCustomLoadCaster(a1) as tupleA;"
            + "C = foreach B GENERATE (chararray) tupleA.a1;" //using loadcaster
            + "store C into 'output' using mock.Storage();";

        pig.registerQuery(query);
        pig.executeBatch();

        LogicalPlan lp = Util.parse(query, pig.getPigContext());
        Util.optimizeNewLP(lp);

        CastFinder cf = new CastFinder(lp);
        cf.visit();
        Assert.assertEquals("There should be only one typecast expression.", 1, cf.casts.size());
        Assert.assertEquals("Loadcaster should be coming from the UDF", "org.apache.pig.test.TestLineageFindRelVisitor$ToTupleWithCustomLoadCaster", cf.casts.get(0).getFuncSpec().getClassName());

        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {"('aaa')", "('bbb')"});
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testUDFForwardingLoadCasterWithMultipleParams() throws Exception{
        File inputfile = Util.createFile(new String[]{"123","456","789"});

        pig.registerQuery("A = load '"
                + inputfile.toString()
                + "' using PigStorage() as (a1:bytearray);\n");
        pig.registerQuery("B = load '"
                + inputfile.toString()
                + "' using PigStorage() as (b1:bytearray);\n");
        pig.registerQuery("C = join A by a1, B by b1;\n");
        pig.registerQuery("D = FOREACH C GENERATE TOTUPLE(a1,b1) as tupleD;\n");
        pig.registerQuery("E = FOREACH D GENERATE (chararray) tupleD.a1;\n");
        Iterator<Tuple> iter  = pig.openIterator("E");

        Assert.assertEquals("123", iter.next().get(0));
        Assert.assertEquals("456", iter.next().get(0));
        Assert.assertEquals("789", iter.next().get(0));
    }

    @Test
    public void testNegativeUDFForwardingLoadCasterWithMultipleParams() throws Exception {
        File inputfile = Util.createFile(new String[]{"123","456","789"});

        pig.registerQuery("A = load '"
                + inputfile.toString()
                + "' using PigStorage() as (a1:bytearray);\n");
        pig.registerQuery("B = load '"
                + inputfile.toString()
                + "' using org.apache.pig.test.TestLineageFindRelVisitor$SillyLoaderWithLoadCasterWithExtraConstructor2() as (b1:bytearray);\n");
        pig.registerQuery("C = join A by a1, B by b1;\n");
        pig.registerQuery("D = FOREACH C GENERATE TOTUPLE(a1,b1) as tupleD;\n");
        pig.registerQuery("E = FOREACH D GENERATE (chararray) tupleD.a1;\n");
        try {
            Iterator<Tuple> iter  = pig.openIterator("E");

            // this should fail since above typecast cannot determine which
            // loadcaster to use (one from PigStroage and another from
            // SillyLoaderWithLoadCasterWithExtraConstructor2)
            fail("Above typecast should fail since it cannot determine which loadcaster to use.");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Unable to open iterator for alias E"));
        }


    }

    /**
     * Find all casts in the plan (Copied from TestTypeCheckingValidatorNewLP.java)
     */
    class CastFinder extends AllExpressionVisitor {
        List<CastExpression> casts = new ArrayList<CastExpression>();

        public CastFinder(OperatorPlan plan)
                throws FrontendException {
            super(plan, new DependencyOrderWalker(plan));
        }

        @Override
        protected LogicalExpressionVisitor getVisitor(
                LogicalExpressionPlan exprPlan) throws FrontendException {
            return new CastExpFinder(exprPlan, new ReverseDependencyOrderWalker(exprPlan));
        }

        class CastExpFinder extends LogicalExpressionVisitor{
            protected CastExpFinder(OperatorPlan p, PlanWalker walker)
            throws FrontendException {
                super(p, walker);
            }

            @Override
            public void visit(CastExpression cExp){
                casts.add(cExp);
            }
        }
    }
}
