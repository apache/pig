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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORead;
import org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

public class TestPOCross extends TestCase {

    DataBag [] inputs = new DataBag[2];
    Random r = new Random();
    
    @Before
    public void setUp() {
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(1);
        inputs[0] = BagFactory.getInstance().newDefaultBag();
        inputs[0].add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(2);
        inputs[0].add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(3);
        inputs[0].add(t);
        
        t = TupleFactory.getInstance().newTuple();
        t.append(5);
        inputs[1] = BagFactory.getInstance().newDefaultBag();
        inputs[1].add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(6);
        inputs[1].add(t);
        
        
    }
    
    @Test
    public void testCross() throws ExecException {
        PORead pr1 = new PORead(new OperatorKey("", r.nextLong()), inputs[0]);
        PORead pr2 = new PORead(new OperatorKey("", r.nextLong()), inputs[1]);
        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(pr1);
        inputs.add(pr2);
        
        //create the expected data bag
        DataBag expected = BagFactory.getInstance().newDefaultBag();
        //List<Tuple> expected = new LinkedList<Tuple>();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(1);
        t.append(5);
        expected.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(2);
        t.append(5);
        expected.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(3);
        t.append(5);
        expected.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(1);
        t.append(6);
        expected.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(2);
        t.append(6);
        expected.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(3);
        t.append(6);
        expected.add(t);
        
        POCross poc = new POCross(new OperatorKey("", r.nextLong()), inputs);
        DataBag obtained = BagFactory.getInstance().newDefaultBag();
        
        for(Result res = poc.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = poc.getNext(t)) {
            if(res.returnStatus == POStatus.STATUS_OK) 
                obtained.add((Tuple) res.result);
            System.out.println(res.result);
        }
        
        assertEquals(expected.size(), obtained.size());
        
        assertEquals(0, DataType.compare(expected, obtained));
        
        
    }
}
