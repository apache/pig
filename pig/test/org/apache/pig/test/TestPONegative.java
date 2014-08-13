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
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.junit.Test;
public class TestPONegative {
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    Random r = new Random(100L);
    TupleFactory tf = TupleFactory.getInstance();
    final int MAX = 10;

    @Test
    public void testPONegInt () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt());
            bag.add(t);
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.INTEGER);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.INTEGER);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);
            Integer expected = -(Integer)t.get(0);
            Integer output = (Integer) pn.getNextInteger().result;
            assertEquals(expected, output);
        }
    }

    @Test
    public void testPONegIntAndNull () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt());
            bag.add(t);
            if( r.nextInt(3) % 3 == 0 ){
                t = tf.newTuple();
                t.append(null);
                bag.add(t);
            }
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.INTEGER);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.INTEGER);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);

            if(t.get(0) == null) {
                Integer output = (Integer)pn.getNextInteger().result;
                assertNull(output);
            } else  {
                Integer expected = -(Integer)t.get(0);
                Integer output = (Integer) pn.getNextInteger().result;
                assertEquals(expected, output);
            }
          }
    }

    @Test
    public void testPONegLong () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextLong());
            bag.add(t);
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.LONG);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.LONG);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);
            Long expected = -(Long)t.get(0);
            long output = (Long) pn.getNextLong().result;
            assertEquals(expected.longValue(), output);
        }
    }

    @Test
    public void testPONegLongAndNull () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextLong());
            bag.add(t);
            if( r.nextInt(3) % 3 == 0 ){
                t = tf.newTuple();
                t.append(null);
                bag.add(t);
            }
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.LONG);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.LONG);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);

            if(t.get(0) == null) {
                Long output = (Long)pn.getNextLong().result;
                assertNull(output);
            } else  {
                Long expected = -(Long)t.get(0);
                long output = (Long) pn.getNextLong().result;
                assertEquals(expected.longValue(), output);
            }
        }
    }

    @Test
    public void testPONegDouble() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextDouble());
            bag.add(t);
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.DOUBLE);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.DOUBLE);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);
            Double expected = -(Double)t.get(0);
            Double output = (Double) pn.getNextDouble().result;
            assertEquals(expected, output);
        }
    }

    @Test
    public void testPONegDoubleAndNull() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextDouble());
            bag.add(t);
            if( r.nextInt(3) % 3 == 0 ){
                t = tf.newTuple();
                t.append(null);
                bag.add(t);
            }
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.DOUBLE);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.DOUBLE);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);

            if(t.get(0) == null) {
                Double output = (Double )pn.getNextDouble().result;
                assertNull(output);
            } else  {
                Double expected = -(Double)t.get(0);
                Double output = (Double) pn.getNextDouble().result;
                assertEquals(expected, output);
            }
        }
    }

    @Test
    public void testPONegFloat() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextFloat());
            bag.add(t);
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.FLOAT);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.FLOAT);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for (Tuple t : bag) {
            plan.attachInput(t);
            Float expected = -(Float)t.get(0);
            Float output = (Float) pn.getNextFloat().result;
            assertEquals(expected, output);
        }
    }

    @Test
    public void testPONegFloatAndNull() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextFloat());
            bag.add(t);
            if( r.nextInt(3) % 3 == 0 ){
                t = tf.newTuple();
                t.append(null);
                bag.add(t);
            }
        }

        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.FLOAT);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.FLOAT);

        PhysicalPlan plan = new PhysicalPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);

        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);

            if(t.get(0) == null) {
                Float output = (Float)pn.getNextFloat().result;
                assertNull(output);
            } else  {
                Float expected = -(Float)t.get(0);
                Float output = (Float) pn.getNextFloat().result;
                assertEquals(expected, output);
            }
        }
    }

    @Test
    public void testPONegType() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL, new Properties());
        File f = Util.createInputFile("tmp", "", new String[] {"a", "b", "c"});
        pig.registerQuery("a = load '"
                + Util.encodeEscape(Util.generateURI(f.toString(), pig.getPigContext())) + "';");
        // -1 is modeled as POnegative with Constant(1)
        pig.registerQuery("b = foreach a generate SIZE(-1);");
        Iterator<Tuple> it = pig.openIterator("b");
        int i = 0;
        while(it.hasNext()) {
            assertEquals(1L, it.next().get(0));
            i++;
        }
        assertEquals(3, i);
    }
}
