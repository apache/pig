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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPackage extends junit.framework.TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    static class ITIterator implements Iterator<IndexedTuple>,
            Iterable<IndexedTuple> {
        private Iterator<Tuple> it;

        public ITIterator(Iterator<Tuple> it) {
            this.it = it;
        }

        public boolean hasNext() {
            return it.hasNext();
        }

        public IndexedTuple next() {
            return (IndexedTuple) it.next();
        }

        public void remove() {
            // TODO Auto-generated method stub

        }

        public Iterator<IndexedTuple> iterator() {
            return this;
        }

    }
    
    public static boolean test(Object key, boolean inner[]) throws ExecException, IOException {
        boolean ret = false;
        Random r = new Random();
        DataBag db1 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        DataBag db2 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        DataBag db = DefaultBagFactory.getInstance().newDefaultBag();
        Iterator db1Iter = db1.iterator();
        if(!inner[0]){
            while (db1Iter.hasNext()) {
                IndexedTuple it = new IndexedTuple((Tuple) db1Iter.next(), 0);
                db.add(it);
            }
        }
        Iterator db2Iter = db2.iterator();
        while (db2Iter.hasNext()) {
            IndexedTuple it = new IndexedTuple((Tuple) db2Iter.next(), 1);
            db.add(it);
        }
        ITIterator iti = new TestPackage.ITIterator(db.iterator());
        POPackage pop = new POPackage(new OperatorKey("", r.nextLong()));
        pop.setNumInps(2);
        pop.setInner(inner);
        pop.attachInput(key, iti);
        Tuple t = null;
        Result res = null;
        res = (Result) pop.getNext(t);
        if(res.returnStatus==POStatus.STATUS_NULL && inner[0])
            return true;
        if (res.returnStatus != POStatus.STATUS_OK)
            return false;

        t = (Tuple) res.result;
        Object outKey = t.get(0);
        DataBag outDb1 = (DataBag) t.get(1);
        DataBag outDb2 = (DataBag) t.get(2);

        if (outKey == key && TestHelper.compareBags(db1, outDb1)
                && TestHelper.compareBags(db2, outDb2))
            return true;
        return ret;
    }

    /**
     * To show that it does not have any type specific
     * code
     */
    private static boolean test(byte t, boolean[] inner) throws ExecException, IOException {
        Random r = new Random();
        switch (t) {
        case DataType.BAG:
            return test(GenRandomData.genRandSmallTupDataBag(r, 10, 100),inner);
        case DataType.BOOLEAN:
            return test(r.nextBoolean(),inner);
        case DataType.BYTEARRAY:
            return test(GenRandomData.genRandDBA(r),inner);
        case DataType.CHARARRAY:
            return test(GenRandomData.genRandString(r),inner);
        case DataType.DOUBLE:
            return test(r.nextDouble(),inner);
        case DataType.FLOAT:
            return test(r.nextFloat(),inner);
        case DataType.INTEGER:
            return test(r.nextLong(),inner);
        case DataType.LONG:
            return test(r.nextLong(),inner);
        case DataType.MAP:
            return test(GenRandomData.genRandMap(r, 10),inner);
        case DataType.TUPLE:
            return test(GenRandomData.genRandSmallBagTuple(r, 10, 100),inner);
        }
        return false;
    }

    @Test
    public void testOperator() throws ExecException, IOException{
        byte[] types = DataType.genAllTypes();
//        Map<Byte, String> map = operatorHelper.genTypeToNameMap();
//        System.out.println("Testing Package:");
        for (byte b : types) {
//            System.out.print("\t With " + map.get(b) + ": ");
            boolean succ = true;
            int NUM_TRIALS = 10;
            boolean[] inner1 = { false , false };
            for (int i = 0; i < NUM_TRIALS; i++)
                succ &= test(b, inner1);
            assertEquals(true, succ);
            
            boolean[] inner2 = { true , false };
            for (int i = 0; i < NUM_TRIALS; i++)
                succ &= test(b, inner2);
            assertEquals(true, succ);
            /*if (succ)
                System.out.println("Success!!");
            else
                System.out.println("Failure");*/
        }
    }
}
