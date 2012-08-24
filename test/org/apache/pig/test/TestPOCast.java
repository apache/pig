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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

public class TestPOCast extends TestCase {

	Random r = new Random();
	final int MAX = 10;
	Tuple dummyTuple = null;
	Map<Object,Object> dummyMap = null;
	DataBag dummyBag = null;
	
	@Test
	public void testBooleanToOther() throws IOException {
	    //Create data
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < MAX; i++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(r.nextBoolean());
            bag.add(t);
        }
        
        POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
        LoadFunc load = new TestLoader();
        op.setFuncSpec(new FuncSpec(load.getClass().getName()));
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj);
        plan.add(op);
        plan.connect(prj, op);
        
        prj.setResultType(DataType.BOOLEAN);
        
        // Plan to test when result type is ByteArray and casting is requested
        // for example casting of values coming out of map lookup.
        POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
        PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = (Boolean) t.get(0);
            Result res = op.getNext(b);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(b, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(b, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Integer i = ((Boolean) t.get(0)) ? Integer.valueOf(1) : Integer.valueOf(0);
            Result res = op.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(i, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(i, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Long l = ((Boolean) t.get(0)) ? Long.valueOf(1L) : Long.valueOf(0L);
            Result res = op.getNext(l);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(l, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(l);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(l, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Float f = ((Boolean) t.get(0)) ? Float.valueOf(1.0F) : Float.valueOf(0.0F);
            Result res = op.getNext(f);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(f, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(f);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(f, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Double d = ((Boolean) t.get(0)) ? Double.valueOf(1.0D) : Double.valueOf(0.0D);
            Result res = op.getNext(d);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(d, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(d);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(d, res.result);
            }
        }

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = null;
            Result res = op.getNext(dt);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            String str = ((Boolean)t.get(0)).toString();
            Result res = op.getNext(str);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(str, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(str);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(str, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DataByteArray dba = new DataByteArray(((Boolean)t.get(0)).toString().getBytes());
            Result res = op.getNext(dba);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(dba, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dba);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(dba, res.result);
            }
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Map map = null;
            Result res = op.getNext(map);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Result res = op.getNext(t);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DataBag b = null;
            Result res = op.getNext(b);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}
	
	@Test
	public void testIntegerToOther() throws IOException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i == 0 ? 0 : r.nextInt());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.INTEGER);
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = Boolean.valueOf((Integer) t.get(0) != 0);
            Result res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(b, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(b, res.result);
            }
        }

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = (Integer) t.get(0);
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(i, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Integer)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(f, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(f, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Long l = ((Integer)t.get(0)).longValue();
			Result res = op.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(l, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(l, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Double d = ((Integer)t.get(0)).doubleValue();
			Result res = op.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(d, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(d, res.result);
			}
		}

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = new DateTime(((Integer)t.get(0)).longValue());
            Result res = op.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(dt, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(dt, res.result);
            }
        }
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			String str = ((Integer)t.get(0)).toString();
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(str, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(str, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataByteArray dba = new DataByteArray(((Integer)t.get(0)).toString().getBytes());
			Result res = op.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(dba, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(dba, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}
	
	@Test
	public void testLongToOther() throws IOException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i == 0 ? 0L : r.nextLong());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.LONG);
		
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = Boolean.valueOf(((Long) t.get(0)) != 0L);
            Result res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                // System.out.println(res.result + " : " + i);
                assertEquals(b, res.result);
            }

            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK)
                assertEquals(b, res.result);

        }

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Long) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
			
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Long)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//			   System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(f, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Long l = ((Long)t.get(0)).longValue();
			Result res = op.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + l);
				assertEquals(l, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(l, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Double d = ((Long)t.get(0)).doubleValue();
			Result res = op.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + d);
				assertEquals(d, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(d, res.result);
		}

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = new DateTime((Long)t.get(0));
            Result res = op.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + l);
                assertEquals(dt, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(dt, res.result);
        }

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			String str = ((Long)t.get(0)).toString();
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + str);
				assertEquals(str, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(str, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataByteArray dba = new DataByteArray(((Long)t.get(0)).toString().getBytes());
			Result res = op.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + dba);
				assertEquals(dba, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(dba, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}
	
	@Test
	public void testFloatToOther() throws IOException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i == 0 ? 0.0F : r.nextFloat());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.FLOAT);
		
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = Boolean.valueOf(((Float) t.get(0)) != 0.0F);
            Result res = op.getNext(b);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(b, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(b, res.result);
        }
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Float) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Float)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//			  System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(f, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Long l = ((Float)t.get(0)).longValue();
			Result res = op.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + l);
				assertEquals(l, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(l, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Double d = ((Float)t.get(0)).doubleValue();
			Result res = op.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + d);
				assertEquals(d, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(d, res.result);
		}

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = new DateTime(((Float)t.get(0)).longValue());
            Result res = op.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + dt);
                assertEquals(dt, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(dt, res.result);
        }   

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			String str = ((Float)t.get(0)).toString();
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + str);
				assertEquals(str, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(str, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataByteArray dba = new DataByteArray(((Float)t.get(0)).toString().getBytes());
			Result res = op.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + dba);
				assertEquals(dba, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(dba, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			if(t.get(0) == null) {
			
					  Float result = (Float) op.getNext((Float) null).result;
				assertEquals( null, result);

			}
		}

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}
	
	@Test
	public void testDoubleToOther() throws IOException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i == 0 ? 0.0D : r.nextDouble());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.DOUBLE);
		
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = Boolean.valueOf(((Double) t.get(0)) != 0.0D);
            Result res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                // System.out.println(res.result + " : " + i);
                assertEquals(b, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK)
                assertEquals(b, res.result);
        }

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Double) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Double)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//			  System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(f, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Long l = ((Double)t.get(0)).longValue();
			Result res = op.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + l);
				assertEquals(l, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(l, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Double d = ((Double)t.get(0)).doubleValue();
			Result res = op.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + d);
				assertEquals(d, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(d, res.result);
		}

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = new DateTime(((Double)t.get(0)).longValue());
            Result res = op.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + dt);
                assertEquals(dt, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(dt, res.result);
        }		

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			String str = ((Double)t.get(0)).toString();
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + str);
				assertEquals(str, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(str, res.result);
		}

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataByteArray dba = new DataByteArray(((Double)t.get(0)).toString().getBytes());
			Result res = op.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + dba);
				assertEquals(dba, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(dba, res.result);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		{
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}

    @Test
    public void testDateTimeToOther() throws IOException {
        //Create data
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i = 0; i < MAX; i++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(i == 0 ? new DateTime(0L) : new DateTime(r.nextLong()));
            bag.add(t);
        }
        
        POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
        LoadFunc load = new TestLoader();
        op.setFuncSpec(new FuncSpec(load.getClass().getName()));
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj);
        plan.add(op);
        plan.connect(prj, op);
        
        prj.setResultType(DataType.DATETIME);
        
        // Plan to test when result type is ByteArray and casting is requested
        // for example casting of values coming out of map lookup.
        POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
        PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();
            plan.attachInput(t);
            Boolean b = null;
            Result res = op.getNext(b);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Integer i = new Long(((DateTime) t.get(0)).getMillis()).intValue();
            Result res = op.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(i, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(i, res.result);
            
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Float f = new Float(Long.valueOf(((DateTime) t.get(0)).getMillis()).floatValue());
            Result res = op.getNext(f);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + f);
                assertEquals(f, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(f);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(f, res.result);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Long l = new Long(((DateTime)t.get(0)).getMillis());
            Result res = op.getNext(l);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + l);
                assertEquals(l, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(l);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(l, res.result);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Double d = new Double(Long.valueOf(((DateTime) t.get(0)).getMillis()).doubleValue());
            Result res = op.getNext(d);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + f);
                assertEquals(d, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(d);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(d, res.result);
        }

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DateTime dt = (DateTime)t.get(0);
            Result res = op.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + l);
                assertEquals(dt, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dt);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(dt, res.result);
        }

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            String str = ((DateTime)t.get(0)).toString();
            Result res = op.getNext(str);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + str);
                assertEquals(str, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(str);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(str, res.result);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DataByteArray dba = new DataByteArray(((DateTime)t.get(0)).toString().getBytes());
            Result res = op.getNext(dba);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + dba);
                assertEquals(dba, res.result);
            }
            
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(dba);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(dba, res.result);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Map map = null;
            Result res = op.getNext(map);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Result res = op.getNext(t);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            DataBag b = null;
            Result res = op.getNext(b);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
    }

	@Test
	public void testStringToOther() throws IOException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.CHARARRAY);

		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		
		TupleFactory tf = TupleFactory.getInstance();
		
        {
            Tuple t = tf.newTuple();
            t.append((new Boolean(r.nextBoolean())).toString());
            plan.attachInput(t);
            String str = (String) t.get(0);
            Boolean b = null;
            if (str.equalsIgnoreCase("true")) {
                b = Boolean.TRUE;
            } else if (str.equalsIgnoreCase("false")) {
                b = Boolean.FALSE;
            }
            Result res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                // System.out.println(res.result + " : " + i);
                assertEquals(b, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK)
                assertEquals(b, res.result);

            t = tf.newTuple();
            t.append("neither true nor false");
            plan.attachInput(t);
            res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                // System.out.println(res.result + " : " + i);
                assertEquals(null, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK)
                assertEquals(null, res.result);
        }
		
		{
			Tuple t = tf.newTuple();
			t.append((new Integer(r.nextInt())).toString());
			plan.attachInput(t);
			Integer i = Integer.valueOf(((String) t.get(0)));
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append((new Float(r.nextFloat())).toString());
			plan.attachInput(t);
			Float i = Float.valueOf(((String) t.get(0)));
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append((new Long(r.nextLong())).toString());
			plan.attachInput(t);
			Long i = Long.valueOf(((String) t.get(0)));
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append((new Double(r.nextDouble())).toString());
			plan.attachInput(t);
			Double i = Double.valueOf(((String) t.get(0)));
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}

        {
            Tuple t = tf.newTuple();
            t.append((new DateTime(r.nextLong())).toString());
            plan.attachInput(t);
            DateTime i = new DateTime(((String) t.get(0)));
            Result res = op.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(i, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(i, res.result);
        }

		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			plan.attachInput(t);
			String str = (String) t.get(0);
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + str);
				assertEquals(str, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(str, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
		
			plan.attachInput(t);
			DataByteArray dba = new DataByteArray(((String)t.get(0)).getBytes());
			Result res = op.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + dba);
				assertEquals(dba, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(dba, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		{
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyMap);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyTuple);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
        {
            planToTestBACasts.attachInput(dummyTuple);
            try{
                opWithInputTypeAsBA.getNext(dummyBag);
            }catch (Exception e) {
                assertEquals(ExecException.class, e.getClass());
            }
        }
	}
	
	public static class TestLoader extends LoadFunc implements LoadCaster{
	    
        public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
            
        }
        
        @Override
        public Tuple getNext() throws IOException {
            return null;
        }
        
        public DataBag bytesToBag(byte[] b, ResourceFieldSchema s) throws IOException {
            return null;
        }

        public Boolean bytesToBoolean(byte[] b) throws IOException {
            DataByteArray dba = new DataByteArray(b);
            String str = dba.toString();
            /*
            if(str.length() == 0)
                return new Boolean(false);
            else return new Boolean(true);
            */
            if (str.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (str.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else {
                return null;
            }
        }
        
        public String bytesToCharArray(byte[] b) throws IOException {
            DataByteArray dba = new DataByteArray(b);
            return dba.toString();
        }
        
        public Double bytesToDouble(byte[] b) throws IOException {
            return new Double(Double.valueOf(new DataByteArray(b).toString()));
        }
        
        public Float bytesToFloat(byte[] b) throws IOException {
            return new Float(Float.valueOf(new DataByteArray(b).toString()));
        }
        
        public Integer bytesToInteger(byte[] b) throws IOException {
            return new Integer(Integer.valueOf(new DataByteArray(b).toString()));
        }

        public Long bytesToLong(byte[] b) throws IOException {
            return new Long(Long.valueOf(new DataByteArray(b).toString()));
        }

        public DateTime bytesToDateTime(byte[] b) throws IOException {
            return new DateTime(new DataByteArray(b).toString());
        }

        public Map<String, Object> bytesToMap(byte[] b) throws IOException {
          return null;
        }
        
        public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema s) throws IOException {
            return null;
        }

        public Tuple bytesToTuple(byte[] b, ResourceFieldSchema s) throws IOException {
            return null;
        }

        public byte[] toBytes(DataBag bag) throws IOException {
            return null;
        }
	
        public byte[] toBytes(String s) throws IOException {
            return s.getBytes();
        }
        
        public byte[] toBytes(Double d) throws IOException {
            return d.toString().getBytes();
        }
        
        public byte[] toBytes(Float f) throws IOException {
            return f.toString().getBytes();
        }
        
        public byte[] toBytes(Integer i) throws IOException {
            return i.toString().getBytes();
        }
        
        public byte[] toBytes(Long l) throws IOException {
            return l.toString().getBytes();
        }
        
        public byte[] toBytes(DateTime dt) throws IOException {
            return dt.toString().getBytes();
        }

        public byte[] toBytes(Boolean b) throws IOException {
            return b.toString().getBytes();
        }
        
	    public byte[] toBytes(Map<String, Object> m) throws IOException {
	        return null;
	    }
	
        public byte[] toBytes(Tuple t) throws IOException {
            return null;
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            return null;
        }

        @Override
        public LoadCaster getLoadCaster() throws IOException {
            return this;
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split)
                throws IOException {

        }

        @Override
        public String relativeToAbsolutePath(String location, Path curDir)
                throws IOException {
            return null;
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
 
        }
        
	}
	
	@Test
	public void testByteArrayToOther() throws IOException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setFuncSpec(new FuncSpec(load.getClass().getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.BYTEARRAY);
		
		TupleFactory tf = TupleFactory.getInstance();
		
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
        {
            Tuple t = tf.newTuple();
            t.append(new DataByteArray((new Boolean(r.nextBoolean())).toString()
                    .getBytes()));
            plan.attachInput(t);
            String str = ((DataByteArray) t.get(0)).toString();
            Boolean b = null;
            if (str.equalsIgnoreCase("true")) {
                b = Boolean.TRUE;
            } else if (str.equalsIgnoreCase("false")) {
                b = Boolean.FALSE;
            }
            Result res = op.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                // System.out.println(res.result + " : " + i);
                assertEquals(b, res.result);
            }

            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK)
                assertEquals(b, res.result);

        }

		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray((new Integer(r.nextInt())).toString().getBytes()));
			plan.attachInput(t);
			Integer i = Integer.valueOf(((DataByteArray) t.get(0)).toString());
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);

		}

		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray((new Float(r.nextFloat())).toString().getBytes()));
			plan.attachInput(t);
			Float i = Float.valueOf(((DataByteArray) t.get(0)).toString());
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray((new Long(r.nextLong())).toString().getBytes()));
			plan.attachInput(t);
			Long i = Long.valueOf(((DataByteArray) t.get(0)).toString());
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray((new Double(r.nextDouble())).toString().getBytes()));
			plan.attachInput(t);
			Double i = Double.valueOf(((DataByteArray) t.get(0)).toString());
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(i, res.result);
		}

		{
            Tuple t = tf.newTuple();
            t.append(new DataByteArray((new DateTime(r.nextLong())).toString().getBytes()));
            plan.attachInput(t);
            DateTime i = new DateTime(((DataByteArray) t.get(0)).toString());
            Result res = op.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(i, res.result);
            }
            planToTestBACasts.attachInput(t);
            res = opWithInputTypeAsBA.getNext(i);
            if(res.returnStatus == POStatus.STATUS_OK)
                assertEquals(i, res.result);
        }

		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			plan.attachInput(t);
			String str = ((DataByteArray) t.get(0)).toString();
			Result res = op.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + str);
				assertEquals(str, res.result);
			}
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(str);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(str, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			
			plan.attachInput(t);
			DataByteArray dba = (DataByteArray) t.get(0);
			Result res = op.getNext(dba);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(dba);
			if(res.returnStatus == POStatus.STATUS_OK)
				assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			//assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(map);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			plan.attachInput(t);
			Result res = op.getNext(t);
			//assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(t);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			//assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(b);
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(null, res.result);
		}
	}
	
	private PhysicalPlan constructPlan(POCast op) throws IOException {
        LoadFunc load = new TestLoader();
        op.setFuncSpec(new FuncSpec(load.getClass().getName()));
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj);
        plan.add(op);
        plan.connect(prj, op);
        prj.setResultType(DataType.BYTEARRAY);
        return plan;
	}
	
	/* 
     * Test that if the input type is actually same 
     * as output type and we think that the input type is a
     * bytearray we still can handle it. This can happen in the
     * following situation:
     * If a map in pig (say returned from a UDF) has a key with 
     * the value being a string, then a lookup of that key being used
     * in a context which expects a string will cause an implicit cast
     * to a string. This is because the Pig frontend (logical layer) 
     * thinks of all map "values" as bytearrays and hence introduces 
     * a Cast to convert the bytearray to string. Though in reality
     * the input to the cast is already a string
     */
	@Test
	public void testByteArrayToOtherNoCast() throws IOException {
        POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
        PhysicalPlan plan = constructPlan(op);
        TupleFactory tf = TupleFactory.getInstance();
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Boolean input = new Boolean(r.nextBoolean());
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext((Boolean) null);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
            
            t = tf.newTuple();
            t.append("neither true nor false");
            plan.attachInput(t);
            res = newOp.getNext((Boolean) null);
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(null, res.result);
            }
        }
        
        {
            Tuple t = tf.newTuple();
            Integer input = new Integer(r.nextInt()); 
            t.append(input);
            plan.attachInput(t);
            Result res = op.getNext(new Integer(0));
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
        }
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Float input = new Float(r.nextFloat());
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(new Float(0));
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
        }
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Long input = new Long(r.nextLong());
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(new Long(0));
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
        }
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Double input = new Double(r.nextDouble());
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(new Double(0));
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
        }

        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            DateTime input = new DateTime(r.nextLong());
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(new DateTime(0L));
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + i);
                assertEquals(input, res.result);
            }
        }

        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Tuple input = GenRandomData.genRandSmallTuple("test", 1);
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(tf.newTuple());
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + str);
                assertEquals(input, res.result);
            }
        }
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            DataBag input = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(DefaultBagFactory.getInstance().newDefaultBag());
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + str);
                assertEquals(input, res.result);
            }
        }
        
        {
            // create a new POCast each time since we 
            // maintain a state variable per POCast object
            // indicating if cast is really required
            POCast newOp = new POCast(new OperatorKey("", r.nextLong()), -1);
            plan = constructPlan(newOp);
            Tuple t = tf.newTuple();
            Map<String, Object> input = new HashMap<String, Object>();
            input.put("key1", "value1");
            input.put("key2", "value2");
            t.append(input);
            plan.attachInput(t);
            Result res = newOp.getNext(new HashMap<String, Object>());
            if(res.returnStatus == POStatus.STATUS_OK) {
                //System.out.println(res.result + " : " + str);
                assertEquals(input, res.result);
            }
        }
        
	}
	
	@Test
	public void testTupleToOther() throws IOException, ParserException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setFuncSpec(new FuncSpec(PigStorage.class.getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.TUPLE);
		
		TupleFactory tf = TupleFactory.getInstance();
		
		//Plan to test when result type is ByteArray and casting is requested
		//for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Result res = op.getNext(t);
			//System.out.println(res.result + " : " + t);
			assertEquals(t, res.result);
			
			planToTestBACasts.attachInput(tNew);
			res = opWithInputTypeAsBA.getNext(t);
			assertEquals(t, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Integer i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Long i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Float i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			Double i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}

		{
            Tuple t = tf.newTuple();
            t.append(GenRandomData.genRandString(r));
            Tuple tNew = tf.newTuple();
            tNew.append(t);
            plan.attachInput(tNew);
            DateTime dt = null;
            Result res = op.getNext(dt);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			String i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandString(r));
			Tuple tNew = tf.newTuple();
			tNew.append(t);
			plan.attachInput(tNew);
			DataByteArray i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			
			op.setFuncSpec(new FuncSpec(BinStorage.class.getName()));
			plan.attachInput(tNew);
			res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
        {
            Tuple t = tf.newTuple();
            Tuple wrappedTuple = tf.newTuple();
            wrappedTuple.append(GenRandomData.genRandString(r));
            wrappedTuple.append(GenRandomData.genRandString(r));
            t.append(wrappedTuple);
            Schema s = Utils.getSchemaFromString("t:tuple(a:chararray)}");
            op.setFieldSchema(new ResourceSchema.ResourceFieldSchema(s.getField(0)));
            plan.attachInput(t);
            Tuple tup = null;
            Result res = op.getNext(tup);
            
            assertTrue(res.result==null);
        }
        
        {
            //positive test case
            Tuple t = tf.newTuple();
            Tuple wrappedTuple = tf.newTuple();
            wrappedTuple.append(GenRandomData.genRandString(r));
            wrappedTuple.append(GenRandomData.genRandString(r));
            t.append(wrappedTuple);
            Schema s = Utils.getSchemaFromString("t:tuple(a:chararray, b:chararray)");
            op.setFieldSchema(new ResourceSchema.ResourceFieldSchema(s.getField(0)));
            plan.attachInput(t);
            Tuple tup = null;
            Result res = op.getNext(tup);
            verifyResult(res, POStatus.STATUS_OK, wrappedTuple);
        }

        {
            //test case trying with null inside tuple
            Tuple t = tf.newTuple();
            Tuple wrappedTuple = tf.newTuple();
            wrappedTuple.append(GenRandomData.genRandString(r));
            wrappedTuple.append(null);//NULL col inside tuple
            t.append(wrappedTuple);
            Schema s = Utils.getSchemaFromString("t:tuple(a:chararray, b:chararray)");
            op.setFieldSchema(new ResourceSchema.ResourceFieldSchema(s.getField(0)));
            plan.attachInput(t);
            Tuple tup = null;
            Result res = op.getNext(tup);
            verifyResult(res, POStatus.STATUS_OK, wrappedTuple);
        }
	
	
	}
	
	private void verifyResult(Result res, byte status, Object result) {
        assertEquals("result status", status, res.returnStatus);
        assertEquals("result value", result, res.result);        
    }

    @Test
	public void testBagToOther() throws IOException, ParserException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setFuncSpec(new FuncSpec(PigStorage.class.getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.BAG);
		
		TupleFactory tf = TupleFactory.getInstance();
		
		//Plan to test when result type is ByteArray and casting is requested
		//for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			Map map = null;
			Result res = op.getNext(map);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			DataBag b = (DataBag) t.get(0);
			Result res = op.getNext(b);
			//System.out.println(res.result + " : " + t);
			assertEquals(b, res.result);
			
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(b);
			assertEquals(b, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			Integer i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			Long i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));       
			plan.attachInput(t);
			Float i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			Double i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}

        {
            Tuple t = tf.newTuple();
            t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
            plan.attachInput(t);
            DateTime dt = null;
            Result res = op.getNext(dt);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }

		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			String i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandSmallTupDataBag(r, 1, 100));
			plan.attachInput(t);
			DataByteArray i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			
			op.setFuncSpec(new FuncSpec(BinStorage.class.getName()));
			plan.attachInput(t);
			res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
        {
            Tuple t = tf.newTuple();
            t.append(GenRandomData.genRandSmallTupDataBagWithNulls(r, 20, 100));
            Schema s = Utils.getSchemaFromString("b:bag{t:tuple(a:chararray, b:float)}");
            op.setFieldSchema(new ResourceSchema.ResourceFieldSchema(s.getField(0)));
            plan.attachInput(t);
            DataBag db = null;
            Result res = op.getNext(db);
            Iterator<Tuple> expectedBagIterator = ((DataBag)(t.get(0))).iterator();
            Iterator<Tuple> convertedBagIterator = ((DataBag)(res.result)).iterator();
            
            while(expectedBagIterator.hasNext()) {
                Tuple expectedBagTuple = expectedBagIterator.next();
                Tuple convertedBagTuple = convertedBagIterator.next();
                if(expectedBagTuple.get(0) != null){
                    assertTrue(convertedBagTuple.get(0) instanceof String);
                    assertTrue(expectedBagTuple.get(0).equals(convertedBagTuple.get(0)));
                }
                if(expectedBagTuple.get(1) != null){
                    assertTrue(convertedBagTuple.get(1) instanceof Float);
                    assertTrue(((Float)(expectedBagTuple.get(1))).floatValue()==(Float)(convertedBagTuple.get(1)));
                }
                

            }
        }
        
        {
            Tuple t = tf.newTuple();
            t.append(GenRandomData.genRandSmallTupDataBagWithNulls(r, 20, 100));
            Schema s = Utils.getSchemaFromString("b:bag{}");
            op.setFieldSchema(new ResourceSchema.ResourceFieldSchema(s.getField(0)));
            plan.attachInput(t);
            DataBag db = null;
            Result res = op.getNext(db);
            Iterator<Tuple> expectedBagIterator = ((DataBag)(t.get(0))).iterator();
            Iterator<Tuple> convertedBagIterator = ((DataBag)(res.result)).iterator();
            
            while(expectedBagIterator.hasNext()) {
                Tuple expectedBagTuple = expectedBagIterator.next();
                Tuple convertedBagTuple = convertedBagIterator.next();
                
                if(expectedBagTuple.get(0) != null){
                    assertTrue(convertedBagTuple.get(0) instanceof String);
                    assertTrue(expectedBagTuple.get(0).equals(convertedBagTuple.get(0)));
                }
                
                if(expectedBagTuple.get(1) != null){
                    assertTrue(convertedBagTuple.get(1) instanceof Integer);
                    assertTrue(((Integer)(expectedBagTuple.get(1)))==(Integer)(convertedBagTuple.get(1)));
                }

            }
        }
	}
	
	@Test
	public void testMapToOther() throws IOException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setFuncSpec(new FuncSpec(PigStorage.class.getName()));
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		prj.setResultType(DataType.MAP);
		
		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		TupleFactory tf = TupleFactory.getInstance();
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			plan.attachInput(t);
			Map map = (Map) t.get(0);
			Result res = op.getNext(map);
			//System.out.println(res.result + " : " + t);
			assertEquals(map, res.result);
		     
			planToTestBACasts.attachInput(t);
			res = opWithInputTypeAsBA.getNext(map);
			assertEquals(map, res.result);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			plan.attachInput(t);
			Result res = op.getNext(t);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			plan.attachInput(t);
			DataBag b = null;
			Result res = op.getNext(b);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
			
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			Integer i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{     
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			Long i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			Float i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			Double i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}

        {     
            Tuple t = tf.newTuple();
            t.append(GenRandomData.genRandMap(r, 10));
            DateTime dt = null;
            Result res = op.getNext(dt);
            assertEquals(POStatus.STATUS_ERR, res.returnStatus);
        }

		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			String i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			plan.attachInput(t);
			DataByteArray i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);

			op.setFuncSpec(new FuncSpec(BinStorage.class.getName()));
			plan.attachInput(t);
			res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
	}
	
	@Test
	public void testNullToOther() throws PlanException, ExecException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(r.nextInt());
			bag.add(t);
			if(r.nextInt(3) % 3 == 0) {
				t = TupleFactory.getInstance().newTuple();
				t.append(null);
				bag.add(t);
			}
			
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();       
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
        prj.setResultType(DataType.BOOLEAN);

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            Tuple t = it.next();
            plan.attachInput(t);
            if (t.get(0) == null) {

                Boolean result = (Boolean) op.getNext((Boolean) null).result;
                assertEquals(null, result);

            }

        }

        prj.setResultType(DataType.INTEGER); 
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			if(t.get(0) == null) {
				
				Integer result  = (Integer)op.getNext((Integer)null).result;
				assertEquals( null, result);

			} 
			
		}
		
		prj.setResultType(DataType.FLOAT);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			if(t.get(0) == null) {
				
				Integer result  = (Integer)op.getNext((Integer)null).result;
				assertEquals( null, result);

			} 
			
		}
		
		prj.setResultType(DataType.DOUBLE);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			if(t.get(0) == null) {
				
				Double result = (Double) op.getNext((Double) null).result;
			assertEquals(null, result);

			}
		}

        prj.setResultType(DataType.DATETIME); 
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            if(t.get(0) == null) {
                
                DateTime result  = (DateTime)op.getNext((DateTime)null).result;
                assertEquals( null, result);

            } 
            
        }	

		prj.setResultType(DataType.CHARARRAY);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			if(t.get(0) == null) {
				
				String result  = (String)op.getNext((String)null).result;
				assertEquals( null, result);

			} 
			
		}
		
		prj.setResultType(DataType.BYTEARRAY);
		
		TupleFactory tf = TupleFactory.getInstance();
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray((new Integer(r.nextInt())).toString().getBytes()));
			plan.attachInput(t);
			if(t.get(0) == null) {
				
				DataByteArray result = (DataByteArray) op.getNext((String) null).result;
				assertEquals(null, result);
				
			}
			
		}
		
	}
	
	@Test
	public void testValueTypesChanged() throws IOException {

		// Plan to test when result type is ByteArray and casting is requested
		// for example casting of values coming out of map lookup.
		POCast opWithInputTypeAsBA = new POCast(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan planToTestBACasts = constructPlan(opWithInputTypeAsBA);
		DataBag bag = BagFactory.getInstance().newDefaultBag();

		// Create a bag having tuples having values of different types.
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			if (i % 6 == 0)
			    t.append(r.nextBoolean());
			if(i % 6 == 1)
				t.append(r.nextInt());
			if(i % 6 == 2)
				t.append(r.nextLong());
			if(i % 6 == 3)
				t.append(r.nextDouble());
			if(i % 6 == 4)
				t.append(r.nextFloat());
			if(i % 6 == 5)
			    t.append(r.nextLong());
			bag.add(t);
		}

		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			planToTestBACasts.attachInput(t);
			Object toCast = t.get(0);
            Boolean b = DataType.toBoolean(toCast);
            Result res = opWithInputTypeAsBA.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK) {
                assertEquals(b, res.result);
            }
			Integer i = DataType.toInteger(toCast);
			res = opWithInputTypeAsBA.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(i, res.result);
			}
			Long l = DataType.toLong(toCast);
			res = opWithInputTypeAsBA.getNext(l);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(l, res.result);
			}

			Float f = DataType.toFloat(toCast);
			res = opWithInputTypeAsBA.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(f, res.result);
			}

			Double d = DataType.toDouble(toCast);
			res = opWithInputTypeAsBA.getNext(d);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(d, res.result);
			}

			if (!(toCast instanceof Boolean)) {
                DateTime dt = DataType.toDateTime(toCast);
                res = opWithInputTypeAsBA.getNext(dt);
                if(res.returnStatus == POStatus.STATUS_OK) {
                    assertEquals(dt, res.result);
                }
			}

			String s = DataType.toString(toCast);
			res = opWithInputTypeAsBA.getNext(s);
			if(res.returnStatus == POStatus.STATUS_OK) {
				assertEquals(s, res.result);
			}
		}
	}
}
