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
package org.apache.pig.piggybank.test.evaluation.datetime.diff;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.datetime.diff.*;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class TestDiffDateTime {
    
    @Test
    public void testYearDiff() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2009-01-07T01:07:01.000Z");
        t1.set(1, "2002-01-01T00:00:00.000Z");

        ISOYearsBetween func = new ISOYearsBetween();
        Long years = func.exec(t1);

        System.out.println("Years: " + years.toString());

        Assert.assertTrue(years == 7L);
    }

    @Test
    public void testMonthDiff() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2009-01-07T00:00:00.000Z");
        t1.set(1, "2002-01-01T00:00:00.000Z");

        ISOMonthsBetween func = new ISOMonthsBetween();
        Long months = func.exec(t1);

        System.out.println("Months: " + months.toString());

        Assert.assertTrue(months == 84L);
    }

    @Test
    public void testDayDiff() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2009-01-07T00:00:00.000Z");
        t1.set(1, "2002-01-01T00:00:00.000Z");

        ISODaysBetween func = new ISODaysBetween();
        Long days = func.exec(t1);

        System.out.println("Days: " + days.toString());

        Assert.assertTrue(days == 2563L);
    }

    @Test
    public void testMinuteDiff() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2009-01-07T00:00:00.000Z");
        t1.set(1, "2002-01-01T00:00:00.000Z");

        ISOMinutesBetween func = new ISOMinutesBetween();
        Long mins = func.exec(t1);

        System.out.println("Minutes: " + mins.toString());

        Assert.assertTrue(mins == 3690720L);
    }

    @Test
    public void testSecondsDiff() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2009-01-07T00:00:00.000Z");
        t1.set(1, "2002-01-01T00:00:00.000Z");

        ISOSecondsBetween func = new ISOSecondsBetween();
        Long secs = func.exec(t1);

        System.out.println("Seconds: " + secs.toString());

        Assert.assertTrue(secs == 221443200L);
    }
}
