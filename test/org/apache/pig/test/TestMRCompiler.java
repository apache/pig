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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.mapReduceLayer.MRCompiler;
import org.apache.pig.impl.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.plans.PlanPrinter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSplit;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMRCompiler extends junit.framework.TestCase {
    static PhysicalPlan<PhysicalOperator> php = new PhysicalPlan<PhysicalOperator>();

    MiniCluster cluster = MiniCluster.buildCluster();
    
    static PigContext pc;

    static String[] tests;

    static final int MAX_SIZE = 100000;

    static final long SEED = 1013;
    
    static{
        pc = new PigContext();
        try {
            pc.connect();
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws ExecException {
        GenPhyOp.setR(new Random(SEED));
        
        GenPhyOp.setPc(pc);
        int numTests = 14;
        tests = new String[numTests];
        int cnt = -1;

        for (int i = 1; i <= 9; i++)
            tests[++cnt] = "intTestSim" + i;
        tests[++cnt] = "intTestRun1";
        tests[++cnt] = "intTestRun2";
        for (int i = 1; i <= 3; i++)
            tests[++cnt] = "intTestSpl" + i;
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCompile() throws ExecException, VisitorException,
            ExecException, PlanException, SecurityException,
            IllegalArgumentException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, IOException {
        setUp();
//        generate();
        intTest();
    }

    public static void intTestRun1() throws ExecException, PlanException, VisitorException,
            ExecException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> part1 = new PhysicalPlan<PhysicalOperator>();
        POLoad lC = GenPhyOp.topLoadOp();
        POFilter fC = GenPhyOp.topFilterOp();
        POLocalRearrange lrC = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange grC = GenPhyOp.topGlobalRearrangeOp();
        POPackage pkC = GenPhyOp.topPackageOp();
        part1.add(lC);
        part1.add(fC);
        part1.connect(lC, fC);
        part1.add(lrC);
        part1.connect(fC, lrC);
        part1.add(grC);
        part1.connect(lrC, grC);
        part1.add(pkC);
        part1.connect(grC, pkC);

        POPackage pkD = GenPhyOp.topPackageOp();
        POLocalRearrange lrD = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange grD = GenPhyOp.topGlobalRearrangeOp();
        POLoad lD = GenPhyOp.topLoadOp();
        part1.add(lD);
        part1.add(lrD);
        part1.connect(lD, lrD);

        part1.add(grD);
        part1.connect(lrD, grD);
        part1.add(pkD);
        part1.connect(grD, pkD);

        POPackage pkCD = GenPhyOp.topPackageOp();
        POLocalRearrange lrCD1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lrCD2 = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange grCD = GenPhyOp.topGlobalRearrangeOp();
        part1.add(lrCD1);
        part1.add(lrCD2);
        part1.connect(pkC, lrCD1);
        part1.connect(pkD, lrCD2);
        part1.add(grCD);
        part1.connect(lrCD1, grCD);
        part1.connect(lrCD2, grCD);
        part1.add(pkCD);
        part1.connect(grCD, pkCD);

        POLoad lA = GenPhyOp.topLoadOp();
        POLoad lB = GenPhyOp.topLoadOp();

        // POLoad lC = lA;
        POFilter fA = GenPhyOp.topFilterOp();

        POLocalRearrange lrA = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lrB = GenPhyOp.topLocalRearrangeOp();

        POGlobalRearrange grAB = GenPhyOp.topGlobalRearrangeOp();

        POPackage pkAB = GenPhyOp.topPackageOp();

        POFilter fAB = GenPhyOp.topFilterOp();
        POUnion unABC = GenPhyOp.topUnionOp();

        php.add(lA);
        php.add(lB);

        php.add(fA);

        php.connect(lA, fA);

        php.add(lrA);
        php.add(lrB);

        php.connect(fA, lrA);
        php.connect(lB, lrB);

        php.add(grAB);
        php.connect(lrA, grAB);
        php.connect(lrB, grAB);

        php.add(pkAB);
        php.connect(grAB, pkAB);

        php.add(fAB);
        php.connect(pkAB, fAB);

        php.merge(part1);

        List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>();
        for (PhysicalOperator phyOp : php.getLeaves()) {
            leaves.add(phyOp);
        }

        php.add(unABC);
        for (PhysicalOperator physicalOperator : leaves) {
            php.connect(physicalOperator, unABC);
        }

        POStore st = GenPhyOp.topStoreOp();

        php.add(st);
        php.connect(unABC, st);

    }

    public static void intTestRun2() throws ExecException, PlanException, VisitorException,
            ExecException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> part1 = new PhysicalPlan<PhysicalOperator>();
        POLoad lC = GenPhyOp.topLoadOp();
        POFilter fC = GenPhyOp.topFilterOp();
        POLocalRearrange lrC = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange grC = GenPhyOp.topGlobalRearrangeOp();
        POPackage pkC = GenPhyOp.topPackageOp();
        part1.add(lC);
        part1.add(fC);
        part1.connect(lC, fC);
        part1.add(lrC);
        part1.connect(fC, lrC);
        part1.add(grC);
        part1.connect(lrC, grC);
        part1.add(pkC);
        part1.connect(grC, pkC);

        POPackage pkD = GenPhyOp.topPackageOp();
        POLocalRearrange lrD = GenPhyOp.topLocalRearrangeOp();
        POGlobalRearrange grD = GenPhyOp.topGlobalRearrangeOp();
        POLoad lD = GenPhyOp.topLoadOp();
        part1.add(lD);
        part1.add(lrD);
        part1.connect(lD, lrD);

        part1.add(grD);
        part1.connect(lrD, grD);
        part1.add(pkD);
        part1.connect(grD, pkD);
        part1.connect(pkD, grC);

        POLoad lA = GenPhyOp.topLoadOp();
        POLoad lB = GenPhyOp.topLoadOp();

        // POLoad lC = lA;
        POFilter fA = GenPhyOp.topFilterOp();

        POLocalRearrange lrA = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lrB = GenPhyOp.topLocalRearrangeOp();

        POGlobalRearrange grAB = GenPhyOp.topGlobalRearrangeOp();

        POPackage pkAB = GenPhyOp.topPackageOp();

        POFilter fAB = GenPhyOp.topFilterOp();
        POUnion unABC = GenPhyOp.topUnionOp();

        php.add(lA);
        php.add(lB);

        php.add(fA);

        php.connect(lA, fA);

        php.add(lrA);
        php.add(lrB);

        php.connect(fA, lrA);
        php.connect(lB, lrB);

        php.add(grAB);
        php.connect(lrA, grAB);
        php.connect(lrB, grAB);

        php.add(pkAB);
        php.connect(grAB, pkAB);

        php.add(fAB);
        php.connect(pkAB, fAB);

        php.merge(part1);

        List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>();
        for (PhysicalOperator phyOp : php.getLeaves()) {
            leaves.add(phyOp);
        }

        php.add(unABC);
        for (PhysicalOperator physicalOperator : leaves) {
            php.connect(physicalOperator, unABC);
        }

        POStore st = GenPhyOp.topStoreOp();

        php.add(st);
        php.connect(unABC, st);
    }

    public static void intTestSpl1() throws ExecException, VisitorException, PlanException,
            ExecException, IOException {
        php = new PhysicalPlan<PhysicalOperator>();

        POLoad lA = GenPhyOp.topLoadOp();
        POSplit spl = GenPhyOp.topSplitOp();
        php.add(lA);
        php.add(spl);
        php.connect(lA, spl);

        POFilter fl1 = GenPhyOp.topFilterOp();
        POFilter fl2 = GenPhyOp.topFilterOp();
        php.add(fl1);
        php.add(fl2);
        php.connect(spl, fl1);
        php.connect(spl, fl2);

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr2 = GenPhyOp.topLocalRearrangeOp();
        php.add(lr1);
        php.add(lr2);
        php.connect(fl1, lr1);
        php.connect(fl2, lr2);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.add(gr);
        php.connect(lr1, gr);
        php.connect(lr2, gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.add(pk);
        php.connect(gr, pk);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);
        php.connect(pk, st);

    }

    public static void intTestSpl2() throws ExecException, VisitorException, PlanException,
            ExecException, IOException {
        php = new PhysicalPlan<PhysicalOperator>();

        POLoad lA = GenPhyOp.topLoadOp();
        POSplit spl = GenPhyOp.topSplitOp();
        php.add(lA);
        php.add(spl);
        php.connect(lA, spl);

        POFilter fl1 = GenPhyOp.topFilterOp();
        POFilter fl2 = GenPhyOp.topFilterOp();
        php.add(fl1);
        php.add(fl2);
        php.connect(spl, fl1);
        php.connect(spl, fl2);

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr2 = GenPhyOp.topLocalRearrangeOp();
        php.add(lr1);
        php.add(lr2);
        php.connect(fl1, lr1);
        php.connect(fl2, lr2);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POSplit sp2 = GenPhyOp.topSplitOp();
        php.addAsLeaf(sp2);

        POFilter fl3 = GenPhyOp.topFilterOp();
        POFilter fl4 = GenPhyOp.topFilterOp();
        php.add(fl3);
        php.add(fl4);
        php.connect(sp2, fl3);
        php.connect(sp2, fl4);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);

    }

    public static void intTestSpl3() throws ExecException, VisitorException, PlanException,
            ExecException, IOException {
        php = new PhysicalPlan<PhysicalOperator>();

        POLoad lA = GenPhyOp.topLoadOp();
        POSplit spl = GenPhyOp.topSplitOp();
        php.add(lA);
        php.add(spl);
        php.connect(lA, spl);

        POFilter fl1 = GenPhyOp.topFilterOp();
        POFilter fl2 = GenPhyOp.topFilterOp();
        php.add(fl1);
        php.add(fl2);
        php.connect(spl, fl1);
        php.connect(spl, fl2);

        POSplit sp11 = GenPhyOp.topSplitOp();
        POSplit sp21 = GenPhyOp.topSplitOp();
        php.add(sp11);
        php.add(sp21);
        php.connect(fl1, sp11);
        php.connect(fl2, sp21);

        POFilter fl11 = GenPhyOp.topFilterOp();
        POFilter fl21 = GenPhyOp.topFilterOp();
        POFilter fl22 = GenPhyOp.topFilterOp();
        php.add(fl11);
        php.add(fl21);
        php.add(fl22);
        php.connect(sp11, fl11);
        php.connect(sp21, fl21);
        php.connect(sp21, fl22);

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr21 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr22 = GenPhyOp.topLocalRearrangeOp();
        php.add(lr1);
        php.add(lr21);
        php.add(lr22);
        php.connect(fl11, lr1);
        php.connect(fl21, lr21);
        php.connect(fl22, lr22);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POSplit sp2 = GenPhyOp.topSplitOp();
        php.addAsLeaf(sp2);

        POFilter fl3 = GenPhyOp.topFilterOp();
        POFilter fl4 = GenPhyOp.topFilterOp();
        php.add(fl3);
        php.add(fl4);
        php.connect(sp2, fl3);
        php.connect(sp2, fl4);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);

    }

    /**
     * Tests Single input case for both blocking and non-blocking
     * with both map and reduce phases
     * @throws PlanException
     * @throws ExecException
     */
    public static void intTestSim1() throws PlanException, ExecException {
        php = new PhysicalPlan<PhysicalOperator>();
        POLoad ld = GenPhyOp.topLoadOp();
        php.add(ld);
        PhysicalPlan<PhysicalOperator> grpChain1 = GenPhyOp.grpChain();
        php.merge(grpChain1);

        php.connect(ld, grpChain1.getRoots().get(0));

        PhysicalOperator leaf = php.getLeaves().get(0);

        PhysicalPlan<PhysicalOperator> grpChain2 = GenPhyOp.grpChain();
        php.merge(grpChain2);

        php.connect(leaf, grpChain2.getRoots().get(0));

        leaf = php.getLeaves().get(0);
        POFilter fl = GenPhyOp.topFilterOp();
        php.add(fl);

        php.connect(leaf, fl);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);

        php.connect(fl, st);

    }

    public static void intTestSim2() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);

        php.connect(un, st);

    }

    public static void intTestSim3() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();

        php.merge(ldFil1);
        php.connect(ldFil1.getLeaves().get(0), un);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);

        php.connect(un, st);
    }

    public static void intTestSim4() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();
        PhysicalPlan<PhysicalOperator> ldFil2 = GenPhyOp.loadedFilter();

        php.merge(ldFil1);
        php.connect(ldFil1.getLeaves().get(0), un);

        php.merge(ldFil2);
        php.connect(ldFil2.getLeaves().get(0), un);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);

        php.connect(un, st);
    }

    public static void intTestSim5() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();
        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();
        PhysicalPlan<PhysicalOperator> ldFil2 = GenPhyOp.loadedFilter();
        php.merge(ldFil1);
        php.merge(ldFil2);

        POUnion un = GenPhyOp.topUnionOp();
        php.addAsLeaf(un);

        POStore st = GenPhyOp.topStoreOp();
        php.add(st);

        php.connect(un, st);
    }

    public static void intTestSim6() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr2 = GenPhyOp.topLocalRearrangeOp();

        ldGrpChain1.addAsLeaf(lr1);
        ldGrpChain2.addAsLeaf(lr2);

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);

    }

    public static void intTestSim7() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr2 = GenPhyOp.topLocalRearrangeOp();

        ldGrpChain1.addAsLeaf(lr1);
        ldGrpChain2.addAsLeaf(lr2);

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();

        php.merge(ldFil1);
        php.connect(ldFil1.getLeaves().get(0), gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);
    }

    public static void intTestSim8() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        PhysicalPlan<PhysicalOperator> ldGrpChain1 = GenPhyOp.loadedGrpChain();
        PhysicalPlan<PhysicalOperator> ldGrpChain2 = GenPhyOp.loadedGrpChain();

        POLocalRearrange lr1 = GenPhyOp.topLocalRearrangeOp();
        POLocalRearrange lr2 = GenPhyOp.topLocalRearrangeOp();

        ldGrpChain1.addAsLeaf(lr1);
        ldGrpChain2.addAsLeaf(lr2);

        php.merge(ldGrpChain1);
        php.merge(ldGrpChain2);

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();
        PhysicalPlan<PhysicalOperator> ldFil2 = GenPhyOp.loadedFilter();

        php.merge(ldFil1);
        php.connect(ldFil1.getLeaves().get(0), gr);

        php.merge(ldFil2);
        php.connect(ldFil2.getLeaves().get(0), gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);
    }

    public static void intTestSim9() throws ExecException, PlanException {
        php = new PhysicalPlan<PhysicalOperator>();

        POGlobalRearrange gr = GenPhyOp.topGlobalRearrangeOp();
        php.addAsLeaf(gr);

        PhysicalPlan<PhysicalOperator> ldFil1 = GenPhyOp.loadedFilter();
        PhysicalPlan<PhysicalOperator> ldFil2 = GenPhyOp.loadedFilter();

        php.merge(ldFil1);
        php.connect(ldFil1.getLeaves().get(0), gr);

        php.merge(ldFil2);
        php.connect(ldFil2.getLeaves().get(0), gr);

        POPackage pk = GenPhyOp.topPackageOp();
        php.addAsLeaf(pk);

        POStore st = GenPhyOp.topStoreOp();
        php.addAsLeaf(st);
    }

    public static void generate() throws SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, VisitorException, ExecException,
            PlanException, IOException {
        for (int i = 0; i < tests.length; i++) {
            Method m = TestMRCompiler.class.getMethod(tests[i], null);
            m.invoke(null, null);

            System.out.println("The plan being compiled under " + tests[i]
                    + ":");
            PlanPrinter phpp = new PlanPrinter(php);
            phpp.visit();

            System.out.println();
            System.out.println("The compiled plan:");

            MRCompiler comp = new MRCompiler(php, pc);
            comp.compile();

            MROperPlan mrp = comp.getMRPlan();
            PlanPrinter ppp = new PlanPrinter(mrp);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ppp.print(baos);

            FileOutputStream fos = new FileOutputStream("intTest/org/apache/pig/test/data/GoldenFiles/MRC"
                    + (i + 1) + ".gld");
            fos.write(baos.toByteArray());

            System.out.println(baos.toString());
            System.out.println("--------------------------------------");
        }
    }

    public static void intTest() throws SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, ExecException, PlanException,
            VisitorException, IOException {
        String compiledPlan, goldenPlan = null;
        int MAX_SIZE = 100000;
        for (int i = 0; i < tests.length; i++) {
            Method m = TestMRCompiler.class.getMethod(tests[i], null);
            m.invoke(null, null);
            MRCompiler comp = new MRCompiler(php, pc);
            comp.compile();

            MROperPlan mrp = comp.getMRPlan();
            PlanPrinter ppp = new PlanPrinter(mrp);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ppp.print(baos);
            compiledPlan = baos.toString();

            FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/MRC"
                    + (i + 1) + ".gld");
            byte[] b = new byte[MAX_SIZE];
            int len = fis.read(b);
            goldenPlan = new String(b, 0, len);

            php.explain(System.out);
            System.out.println();
            System.out.println(compiledPlan);
            System.out.println("-------------");
            //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
            assertEquals(true, compiledPlan.compareTo(goldenPlan) == 0);
        }
    }

    

}
