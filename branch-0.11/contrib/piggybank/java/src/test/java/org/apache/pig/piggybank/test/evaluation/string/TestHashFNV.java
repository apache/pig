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
package org.apache.pig.piggybank.test.evaluation.string;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.HashFNV1;
import org.apache.pig.piggybank.evaluation.string.HashFNV2;
import org.junit.Test;

import junit.framework.TestCase;

public class TestHashFNV extends TestCase {
    @Test
    public void testHashFNV() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "0000000000065&f=a&br=65");
        t1.set(1, 10000);
        
        Tuple t2 = TupleFactory.getInstance().newTuple(2);
        t2.set(0, "024ulhl0dq1tl&b=2");
        t2.set(1, 100);
        
        Tuple t3 = TupleFactory.getInstance().newTuple(2);
        t3.set(0, null);
        t3.set(1, 100);
        
        Tuple t4 = TupleFactory.getInstance().newTuple(1);
        t4.set(0, "024ulhl0dq1tl&b=2");
        
        HashFNV2 func2 = new HashFNV2();
        Long r = func2.exec(t1);
        assertTrue(r==6228);
        r = func2.exec(t2);
        assertTrue(r==31);
        r = func2.exec(t3);
        assertTrue(r==null);
        
        HashFNV1 func1 = new HashFNV1();
        r = func1.exec(t4);
        assertTrue(r==1669505231);
    }
}
