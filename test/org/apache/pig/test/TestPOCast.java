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
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.BinStorage;
import org.junit.Test;

import junit.framework.TestCase;

public class TestPOCast extends TestCase {

	Random r = new Random();
	final int MAX = 10;
	
	@Test
	public void testIntegerToOther() throws PlanException, ExecException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(r.nextInt());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.INTEGER);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = (Integer) t.get(0);
			Result res = op.getNext(i);
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
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Long l = ((Integer)t.get(0)).longValue();
			Result res = op.getNext(l);
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
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			String str = ((Integer)t.get(0)).toString();
			Result res = op.getNext(str);
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
	}
	
	@Test
	public void testLongToOther() throws PlanException, ExecException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(r.nextLong());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.LONG);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Long) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Long)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//				System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
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
	}
	
	@Test
	public void testFloatToOther() throws PlanException, ExecException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(r.nextFloat());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.FLOAT);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Float) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Float)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//				System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
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
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
            if(t.get(0) == null) {
            	
               	Float result  = (Float)op.getNext((Float)null).result;
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
	}
	
	@Test
	public void testDoubleToOther() throws PlanException, ExecException {
		//Create data
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for(int i = 0; i < MAX; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(r.nextDouble());
			bag.add(t);
		}
		
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.DOUBLE);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Integer i = ((Double) t.get(0)).intValue();
			Result res = op.getNext(i);
			if(res.returnStatus == POStatus.STATUS_OK) {
				//System.out.println(res.result + " : " + i);
				assertEquals(i, res.result);
			}
		}
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
			Float f = ((Double)t.get(0)).floatValue();
			Result res = op.getNext(f);
			if(res.returnStatus == POStatus.STATUS_OK) {
//				System.out.println(res.result + " : " + f);
				assertEquals(f, res.result);
			}
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
	}
	
	@Test
	public void testStringToOther() throws PlanException, ExecException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.CHARARRAY);
		
		TupleFactory tf = TupleFactory.getInstance();
		
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
	}
	
	public static class TestLoader implements LoadFunc{
        public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
            
        }
        
        public Tuple getNext() throws IOException {
            return null;
        }
        
        public Schema determineSchema(URL filename) {
            return null;
        }
        
        public void fieldsToRead(Schema schema) {
            
        }
        
        public DataBag bytesToBag(byte[] b) throws IOException {
            return null;
        }

        public Boolean bytesToBoolean(byte[] b) throws IOException {
        	DataByteArray dba = new DataByteArray(b);
        	String str = dba.toString();
        	if(str.length() == 0)
        		return new Boolean(false);
        	else return new Boolean(true);
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

        public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
            return null;
        }

        public Tuple bytesToTuple(byte[] b) throws IOException {
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
	
	    public byte[] toBytes(Map<Object, Object> m) throws IOException {
	        return null;
	    }
	
	    public byte[] toBytes(Tuple t) throws IOException {
	        return null;
	    }
    }
	
	@Test
	public void testByteArrayToOther() throws PlanException, ExecException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		LoadFunc load = new TestLoader();
		op.setLoadFSpec(load.getClass().getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.BYTEARRAY);
		
		TupleFactory tf = TupleFactory.getInstance();
		
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
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
		
			plan.attachInput(t);
			DataByteArray dba = (DataByteArray) t.get(0);
			Result res = op.getNext(dba);
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
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(new DataByteArray(GenRandomData.genRandString(r).getBytes()));
			plan.attachInput(t);
			Result res = op.getNext(t);
			//assertEquals(POStatus.STATUS_ERR, res.returnStatus);
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
		}
	}
	
	@Test
	public void testTupleToOther() throws PlanException, ExecException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setLoadFSpec(PigStorage.class.getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.TUPLE);
		
		TupleFactory tf = TupleFactory.getInstance();
		
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

		    op.setLoadFSpec(BinStorage.class.getName());
			plan.attachInput(tNew);
			res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
	}
	
	@Test
	public void testBagToOther() throws PlanException, ExecException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setLoadFSpec(PigStorage.class.getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.BAG);
		
		TupleFactory tf = TupleFactory.getInstance();
		BagFactory bf = BagFactory.getInstance();
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

		    op.setLoadFSpec(BinStorage.class.getName());
			plan.attachInput(t);
			res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
	}
	
	@Test
	public void testMapToOther() throws PlanException, ExecException {
		POCast op = new POCast(new OperatorKey("", r.nextLong()), -1);
		op.setLoadFSpec(PigStorage.class.getName());
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(prj);
		plan.add(op);
		plan.connect(prj, op);
		
		prj.setResultType(DataType.MAP);
		
		TupleFactory tf = TupleFactory.getInstance();
		BagFactory bf = BagFactory.getInstance();
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandMap(r, 10));
			plan.attachInput(t);
			Map map = (Map) t.get(0);
			Result res = op.getNext(map);
			//System.out.println(res.result + " : " + t);
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
			String i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);
		}
		
		{
			Tuple t = tf.newTuple();
			t.append(GenRandomData.genRandObjectMap(r, 10));
			plan.attachInput(t);
			DataByteArray i = null;
			Result res = op.getNext(i);
			assertEquals(POStatus.STATUS_ERR, res.returnStatus);

		    op.setLoadFSpec(BinStorage.class.getName());
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
            if( r.nextInt(3) % 3 == 0 ){
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
            	
               	Float result  = (Float)op.getNext((Float)null).result;
				assertEquals( null, result);

            } 
		}

		prj.setResultType(DataType.DOUBLE);
		
		for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			plan.attachInput(t);
            if(t.get(0) == null) {
            	
               	Double result  = (Double)op.getNext((Double)null).result;
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
            	
            	DataByteArray result  = (DataByteArray)op.getNext((String)null).result;
				assertEquals( null, result);

            } 

		}


	
	}
}
