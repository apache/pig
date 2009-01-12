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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests localrearrange db for
 * group db by $0 
 *
 */
public class TestLocalRearrange extends junit.framework.TestCase {
    
    POLocalRearrange lr;
    Tuple t;
    DataBag db;
    
    
    @Before
    public void setUp() throws Exception {
        Random r = new Random();
        db = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
    }
    
    private void setUp1() throws PlanException, ExecException{
        lr = GenPhyOp.topLocalRearrangeOPWithPlanPlain(0,0,db.iterator().next());
        POProject proj = GenPhyOp.exprProject();
        proj.setColumn(0);
        proj.setResultType(DataType.TUPLE);
        proj.setOverloaded(true);
        Tuple t = new DefaultTuple();
        t.append(db);
        proj.attachInput(t);
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        inputs.add(proj);
        lr.setInputs(inputs);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNextTuple1() throws ExecException, PlanException {
        setUp1();
        int size=0;
        for(Result res=lr.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=lr.getNext(t)){
            Tuple t = (Tuple)res.result;
            String key = (String)t.get(1);
            Tuple val = (Tuple)t.get(2);
            // The input data has 2 columns of which the first
            // is the key
            // With the optimized LocalRearrange, the part
            // of the "value" present in the "key" is 
            // excluded from the "value". So to reconstruct
            // the true "value", create a tuple with "key" in
            // first position and the "value" (val) we currently
            // have in the second position
            assertEquals(1, val.size());
            
            Tuple actualVal = new DefaultTuple();
            actualVal.append(key);
            actualVal.append(val.get(0));
            //Check if the index is same as input index
            assertEquals((byte)0, (byte)(Byte)t.get(0));
            
            //Check if the input bag contains the value tuple
            assertTrue(TestHelper.bagContains(db, actualVal));
            
            //Check if the input key and the output key are same
            String inpKey = (String)actualVal.get(0);
            assertEquals(0, inpKey.compareTo((String)t.get(1)));
            ++size;
        }
        
        //check if all the tuples in the input are generated
        assertEquals(db.size(), size);
    }
    
    private void setUp2() throws PlanException, ExecException{
        lr = GenPhyOp.topLocalRearrangeOPWithPlanPlain(0,0,db.iterator().next());
        List<PhysicalPlan> plans = lr.getPlans();
        POLocalRearrange lrT = GenPhyOp.topLocalRearrangeOPWithPlanPlain(0, 1, db.iterator().next());
        List<PhysicalPlan> plansT = lrT.getPlans();
        plans.add(plansT.get(0));
        lr.setPlans(plans);
        
        POProject proj = GenPhyOp.exprProject();
        proj.setColumn(0);
        proj.setResultType(DataType.TUPLE);
        proj.setOverloaded(true);
        Tuple t = new DefaultTuple();
        t.append(db);
        proj.attachInput(t);
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        inputs.add(proj);
        lr.setInputs(inputs);
    }
    
    @Test
    public void testGetNextTuple2() throws ExecException, PlanException {
        setUp2();
        int size=0;
        for(Result res=lr.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=lr.getNext(t)){
            Tuple t = (Tuple)res.result;
            Tuple key = (Tuple)t.get(1);
            Tuple val = (Tuple)t.get(2);
            
            // The input data has 2 columns of which both
            // are the key.
            // With the optimized LocalRearrange, the part
            // of the "value" present in the "key" is 
            // excluded from the "value". So in this case, 
            // the "value" coming out of the LocalRearrange
            // would be an empty tuple
            assertEquals(0, val.size());
            
            //Check if the index is same as input index
            assertEquals((byte)0, (byte)(Byte)t.get(0));
            
            // reconstruct value from tuple
            val = key;
            //Check if the input baf contains the value tuple
            assertTrue(TestHelper.bagContains(db, val));
            
            //Check if the input key and the output key are same
            Tuple inpKey = TupleFactory.getInstance().newTuple(2); 
            inpKey.set(0, val.get(0));
            inpKey.set(1, val.get(1));
            assertEquals(0, inpKey.compareTo((Tuple)t.get(1)));
            ++size;
        }
        
        //check if all the tuples in the input are generated
        assertEquals(db.size(), size);
    }

}
