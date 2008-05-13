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

import java.io.File;
import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POForEach;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *  Start Plan - --4430968173902769765
 *  |
 *  |---Filter - -3398344075398026874
 *  |   |
 *  |   |---For Each - --3361918026493682288
 *  |       |
 *  |       |---Load - --7021057205133896020
 *  |
 *  |---Filter - -4449068980110713814
 *      |
 *      |---For Each - -7192652407897311774
 *          |
 *          |---Load - --3816247505117325386
 *          
 *  Tests the Start Plan operator with the above plan.
 *  The verification is done as follows:
 *  Both loads load the same file(/etc/passwd).
 *  The filters cover the input. Here the filters used
 *  are $2<=50 & $2>50
 *  The bags coming out of Start Plan is checked against
 *  the projected input bag.
 *  Since types are not supported yet, there is an explicit
 *  conversion from DataByteArray to native types for computation
 *  and back to DataByteArray for comparison with input.
 */
public class TestUnion extends junit.framework.TestCase {
    POUnion sp;
    DataBag expBag;
    MiniCluster cluster = MiniCluster.buildCluster();
    PigContext pc = new PigContext();
    @Before
    public void setUp() throws Exception {
        pc.connect();
        GenPhyOp.setPc(pc);
        POLoad ld1 = GenPhyOp.topLoadOp();
        String curDir = System.getProperty("user.dir");
        String inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        FileSpec fSpec = new FileSpec("file:"+ inpDir +"passwd",PigStorage.class.getName() + "(':')");
        ld1.setLFile(fSpec);
        
        POLoad ld2 = GenPhyOp.topLoadOp();
        ld2.setLFile(fSpec);
        
        POFilter fl1 = GenPhyOp.topFilterOpWithProj(1, 50, GenPhyOp.LTE);
        
        POFilter fl2 = GenPhyOp.topFilterOpWithProj(1, 50, GenPhyOp.GT);
        
        int[] flds = {0,2};
        Tuple sample = new DefaultTuple();
        sample.append(new String("S"));
        sample.append(new String("x"));
        sample.append(new Integer("10"));
        sample.append(new Integer("20"));
        sample.append(new String("S"));
        sample.append(new String("x"));
        sample.append(new String("S"));
        sample.append(new String("x"));

        POForEach fe1 = GenPhyOp.topForEachOPWithPlan(flds , sample);
        
        POForEach fe2 = GenPhyOp.topForEachOPWithPlan(flds , sample);
        
        sp = GenPhyOp.topUnionOp();
        
        PhysicalPlan<PhysicalOperator> plan = new PhysicalPlan<PhysicalOperator>();
        
        plan.add(ld1);
        plan.add(ld2);
        plan.add(fl1);
        plan.add(fl2);
        plan.add(fe1);
        plan.add(fe2);
        plan.add(sp);
        
        plan.connect(ld1, fe1);
        plan.connect(fe1, fl1);
        plan.connect(ld2, fe2);
        plan.connect(fe2, fl2);
        plan.connect(fl1, sp);
        plan.connect(fl2, sp);
        
        /*PlanPrinter ppp = new PlanPrinter(plan);
        ppp.visit();*/
        
        
        POLoad ld3 = GenPhyOp.topLoadOp();
        ld3.setLFile(fSpec);
        DataBag fullBag = DefaultBagFactory.getInstance().newDefaultBag();
        Tuple t=null;
        for(Result res=ld3.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=ld3.getNext(t)){
            fullBag.add((Tuple)res.result);
        }

        int[] fields = {0,2};
        expBag = TestHelper.projectBag(fullBag, fields);
    }

    @After
    public void tearDown() throws Exception {
    }
    
    private Tuple castToDBA(Tuple in) throws ExecException{
        Tuple res = new DefaultTuple();
        for (int i=0;i<in.size();i++) {
            DataByteArray dba = new DataByteArray(in.get(i).toString());
            res.append(dba);
        }
        return res;
    }

    @Test
    public void testGetNextTuple() throws ExecException, IOException {
        Tuple t = null;
        DataBag outBag = DefaultBagFactory.getInstance().newDefaultBag();
        for(Result res=sp.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=sp.getNext(t)){
            outBag.add(castToDBA((Tuple)res.result));
        }
        assertEquals(true, TestHelper.compareBags(expBag, outBag));
    }

}
