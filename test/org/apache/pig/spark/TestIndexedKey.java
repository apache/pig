/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.spark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.pig.backend.hadoop.executionengine.spark.converter.IndexedKey;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TestIndexedKey {

    /**Case1:Compare IndexedKeys with same index value
     * key1    key2    equal?  hashCode1        hashCode2
     * foo     null      N     hashCode(foo)    index
     * null    foo       N     index            hashCode(foo)
     * foo     foo       Y     hashCode(foo)    hashCode(foo)
     * null    null      Y     index            index
     * (1,1)   (1,1)     Y     hashCode((1,1))  hashCode((1,1))
     * (1,)    (1,)      Y     hashCode((1,))   hashCode((1,))
     * (1,1)   (1,2)     N     hashCode((1,1))  hashCode((1,2))
     */
    @Test
    public void testIndexedKeyWithSameIndexValue() throws Exception {
        IndexedKey a0 = new IndexedKey(new Byte("0"), "foo");
        IndexedKey a1 = new IndexedKey(new Byte("0"), null);
        assertEquals(a0.equals(a1), false);
        assertEquals(a0.hashCode()==a1.hashCode(),false);

        IndexedKey a2 = new IndexedKey(new Byte("0"), null);
        IndexedKey a3 = new IndexedKey(new Byte("0"), "foo");
        assertEquals(a2.equals(a3),false);
        assertEquals(a2.hashCode()==a3.hashCode(),false);

        IndexedKey a4 = new IndexedKey(new Byte("0"), "foo");
        IndexedKey a5 = new IndexedKey(new Byte("0"), "foo");
        assertEquals(a4.equals(a5),true);
        assertEquals(a4.hashCode()==a5.hashCode(),true);

        IndexedKey a6 = new IndexedKey(new Byte("0"), null);
        IndexedKey a7 = new IndexedKey(new Byte("0"), null);
        assertEquals(a6.equals(a7),true);
        assertEquals(a6.hashCode()==a7.hashCode(),true);

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0,"1");
        t1.set(1,"1");
        Tuple t2 = TupleFactory.getInstance().newTuple(2);
        t2.set(0,"1");
        t2.set(1,"1");
        IndexedKey a8 = new IndexedKey(new Byte("0"), t1);
        IndexedKey a9 = new IndexedKey(new Byte("0"), t2);
        assertEquals(a8.equals(a9),true);
        assertEquals(a8.hashCode()==a9.hashCode(),true);

        Tuple t3 = TupleFactory.getInstance().newTuple(2);
        t3.set(0,"1");
        t3.set(1,null);
        Tuple t4 = TupleFactory.getInstance().newTuple(2);
        t4.set(0,"1");
        t4.set(1,null);
        IndexedKey a10 = new IndexedKey(new Byte("0"), t3);
        IndexedKey a11 = new IndexedKey(new Byte("0"), t4);
        assertEquals(a10.equals(a11),true);
        assertEquals(a10.hashCode()==a11.hashCode(),true);

        Tuple t5 = TupleFactory.getInstance().newTuple(2);
        t5.set(0,"1");
        t5.set(1,"1");
        Tuple t6 = TupleFactory.getInstance().newTuple(2);
        t6.set(0,"1");
        t6.set(1,"2");
        IndexedKey a12 = new IndexedKey(new Byte("0"), t5);
        IndexedKey a13 = new IndexedKey(new Byte("0"), t6);
        assertEquals(a12.equals(a13),false);
        assertEquals(a12.hashCode()==a13.hashCode(),false);
    }

    /*
     * Case2:Compare IndexedKeys with different index value
     * key1    key2    equal?  hashCode1        hashCode2
     * foo     null     N      hashCode(foo)    index2
     * null    foo      N      index1           hashCode(foo)
     * foo     foo      Y      hashCode(foo)    hashCode(foo)
     * null    null     N      index1           index2
     * (1,1)   (1,1)    Y      hashCode((1,1))  hashCode((1,1))
     * (1,)    (1,)     N      hashCode((1,))   hashCode((1,))
     * (1,1)   (1,2)    N      hashCode((1,1))  hashCode((1,2))
     */
    @Test
    public void testIndexedKeyWithDifferentIndexValue() throws Exception {
        IndexedKey a0 = new IndexedKey(new Byte("0"), "foo");
        IndexedKey a1 = new IndexedKey(new Byte("1"), null);
        assertEquals(a0.equals(a1), false);
        assertEquals(a0.hashCode() == a1.hashCode(), false);

        IndexedKey a2 = new IndexedKey(new Byte("0"), null);
        IndexedKey a3 = new IndexedKey(new Byte("1"), "foo");
        assertEquals(a2.equals(a3), false);
        assertEquals(a2.hashCode() == a3.hashCode(), false);

        IndexedKey a4 = new IndexedKey(new Byte("0"), "foo");
        IndexedKey a5 = new IndexedKey(new Byte("1"), "foo");
        assertEquals(a4.equals(a5), true);
        assertEquals(a4.hashCode() == a5.hashCode(), true);

        IndexedKey a6 = new IndexedKey(new Byte("0"), null);
        IndexedKey a7 = new IndexedKey(new Byte("1"), null);
        assertEquals(a6.equals(a7), false);
        assertEquals(a6.hashCode() == a7.hashCode(), false);

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "1");
        t1.set(1, "1");
        Tuple t2 = TupleFactory.getInstance().newTuple(2);
        t2.set(0, "1");
        t2.set(1, "1");
        IndexedKey a8 = new IndexedKey(new Byte("0"), t1);
        IndexedKey a9 = new IndexedKey(new Byte("1"), t2);
        assertEquals(a8.equals(a9), true);
        assertEquals(a8.hashCode() == a9.hashCode(), true);

        Tuple t3 = TupleFactory.getInstance().newTuple(2);
        t3.set(0, "1");
        t3.set(1, null);
        Tuple t4 = TupleFactory.getInstance().newTuple(2);
        t4.set(0, "1");
        t4.set(1, null);
        IndexedKey a10 = new IndexedKey(new Byte("0"), t3);
        IndexedKey a11 = new IndexedKey(new Byte("1"), t4);
        assertEquals(a10.equals(a11), false);
        assertEquals(a10.hashCode() == a11.hashCode(), true); //hashcode of a10 and a11 are equal but they are not equal

        Tuple t5 = TupleFactory.getInstance().newTuple(2);
        t5.set(0, "1");
        t5.set(1, "1");
        Tuple t6 = TupleFactory.getInstance().newTuple(2);
        t6.set(0, "1");
        t6.set(1, "2");
        IndexedKey a12 = new IndexedKey(new Byte("0"), t5);
        IndexedKey a13 = new IndexedKey(new Byte("1"), t6);
        assertEquals(a12.equals(a13), false);
        assertEquals(a12.hashCode() == a13.hashCode(), false);
    }
}
