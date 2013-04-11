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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;

public class TestPOSplit {

  private Random r = new Random();

  private Tuple dummyTuple = null;

	@Test
	public void testNegativeErrorAtInputProcessing() throws IOException {
      // This test checks to see if POSplit is catching the error status
      // from input processing. (PIG-3271)

      // First create a operatator that fails when trying
      //    Result res = cast.getNext(dummyTuple);
      // POSplit.processinput would call that line and SHOULD fail.
      POCast cast = new POCast(new OperatorKey("", r.nextLong()), -1);
      POProject proj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
      proj.setResultType(DataType.CHARARRAY);
      List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
      inputs.add(proj);
      cast.setInputs(inputs);
      Tuple tuple = TupleFactory.getInstance().newTuple(1);
      tuple.set(0, "1.2345");
      proj.attachInput(tuple);

      // Create POSplit and attach the above cast op as input
      POSplit split = new POSplit(new OperatorKey("", r.nextLong()), -1);
      inputs = new ArrayList<PhysicalOperator>();
      inputs.add(cast);
      split.setInputs(inputs);

      PhysicalPlan pplan = new PhysicalPlan();
      POStore store = GenPhyOp.dummyPigStorageOp();
      pplan.add(store);

      split.addPlan(pplan);

      // setting parentPlan since POSplit access
      // for "if (this.parentPlan.endOfAllInput)"
      split.setParentPlan(pplan);

      // Calling POSplit which in turn calls POCast and should fail.
      Result res = split.getNext(dummyTuple);

      assertEquals("POSplit should fail when input processing fails.",
                   res.returnStatus, POStatus.STATUS_ERR);
  }
}
