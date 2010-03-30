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
package org.apache.pig.piggybank.test.evaluation.datetime.truncate;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.datetime.truncate.*;
import org.junit.Test;

import junit.framework.TestCase;

public class TestTruncateDateTime extends TestCase {

    @Test
    public void testToYear() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToYear func = new ISOToYear();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-01-01T00:00:00.000Z"));
    }

    @Test
    public void testToMonth() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToMonth func = new ISOToMonth();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-04-01T00:00:00.000Z"));
    }

    @Test
    public void testToWeek() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToWeek func = new ISOToWeek();
        String truncated = func.exec(t1);

        System.out.println("Truncateed week: " + truncated);

        assertTrue(truncated.equals("2010-04-12T00:00:00.000Z"));
    }

    @Test
    public void testToDay() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToDay func = new ISOToDay();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-04-15T00:00:00.000Z"));
    }

    @Test
    public void testToHour() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToHour func = new ISOToHour();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-04-15T08:00:00.000Z"));
    }

    @Test
    public void testToMinute() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToMinute func = new ISOToMinute();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-04-15T08:11:00.000Z"));
    }

    @Test
    public void testToSecond() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToSecond func = new ISOToSecond();
        String truncated = func.exec(t1);

        assertTrue(truncated.equals("2010-04-15T08:11:33.000Z"));
    }
}