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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.datetime.truncate.*;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;
import org.junit.Test;

import junit.framework.TestCase;

public class TestTruncateDateTime extends TestCase {

    @Test
    public void testParseDateTime_defaultTimeZonePreserved() throws ExecException {

        // Remember pre-test default time zone.
        DateTimeZone previousDefaultTimeZone = DateTimeZone.getDefault();

        // Overwrite default time zone for this test.
        DateTimeZone testDefaultDateTimeZone = DateTimeZone.forOffsetHours(-8);
        DateTimeZone.setDefault(testDefaultDateTimeZone);

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020");

        // ISOHelper's internal default timezone is preferred over previous default DateTimeZone.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(2010, 4, 15, 8, 11, 33, 20, ISOHelper.DEFAULT_DATE_TIME_ZONE));

        // Calling parseDate restores DateTimeZone.default before it returns.
        assertTrue(testDefaultDateTimeZone.equals(DateTimeZone.getDefault()));

        // Restore pre-test default time zone.
        DateTimeZone.setDefault(previousDefaultTimeZone);
    }

    @Test
    public void testParseDateTime_UTC() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(2010, 4, 15, 8, 11, 33, 20, DateTimeZone.UTC));

        // Time zone is strictly preserved. Parsed date is not equal to "simultaneous" datetime in different time zone.
        assertFalse(ISOHelper.parseDateTime(t1).equals(new DateTime(2010, 4, 15, 0, 11, 33, 20, DateTimeZone.forOffsetHours(-8))));
    }

    @Test
    public void testParseDateTime_TimeZone() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(2010, 4, 15, 8, 11, 33, 20, DateTimeZone.forOffsetHours(-8)));        

        // Time zone is strictly preserved. Parsed date is not equal to "simultaneous" datetime in different time zone.
        assertFalse(ISOHelper.parseDateTime(t1).equals(new DateTime(2010, 4, 15, 16, 11, 33, 20, DateTimeZone.UTC)));        
    }
    
    /**
     * When no time zone is specified at all, UTC is presumed.
     * @throws Exception
     */
    @Test
    public void testParseDateTime_NoTimeZone() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(2010, 4, 15, 8, 11, 33, 20, DateTimeZone.UTC));
    }

    /**
     * Parsing ISO date with a time zone but no time will throw an exception.
     * @throws Exception
     */
    @Test
    public void testParseDateTime_noTime_UTC() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15Z");

        try {
            ISOHelper.parseDateTime(t1);   
            fail("ISO date with a time zone but no time should not be parsable.");
        } catch (IllegalArgumentException e) {
            // This is expected.
        }
    }
    
    /**
     * Parsing ISO date with no time and no time zone works.
     * Time defaults to midnight in UTC.
     * @throws Exception
     */
    @Test
    public void testParseDateTime_noTime_noTimeZone() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(2010, 4, 15, 0, 0, 0, 0, DateTimeZone.UTC));        
    }
    
    /**
     * Parsing ISO date-less times works. 'T' prefix is required.
     * Date defaults to 1970-01-01
     * @throws Exception
     */
    @Test
    public void testParseDateTime_timeOnly_UTC() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "T08:11:33.020Z");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(1970, 1, 1, 8, 11, 33, 20, DateTimeZone.UTC));        
    }
    
    /**
     * Parsing ISO date-less times works. 'T' prefix is required.
     * Date defaults to 1970-01-01
     * @throws Exception
     */
    @Test
    public void testParseDateTime_timeOnly_TimeZone() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "T08:11:33.020-0800");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(1970, 1, 1, 8, 11, 33, 20, DateTimeZone.forOffsetHours(-8)));        
    }

    /**
     * Parsing ISO date-less times with no time zone works. Defaults to UTC.
     * 'T' prefix is required. Date defaults to 1970-01-01
     * @throws Exception
     */
    @Test
    public void testParseDateTime_timeOnly_NoTimeZone() throws ExecException {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "T08:11:33.020Z");

        // Time zone is preserved.
        assertEquals(ISOHelper.parseDateTime(t1), new DateTime(1970, 1, 1, 8, 11, 33, 20, DateTimeZone.UTC));        
    }
    
    @Test
    public void testToYear() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToYear func = new ISOToYear();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-01-01T00:00:00.000Z");
    }

    @Test
    public void testToMonth() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToMonth func = new ISOToMonth();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-01T00:00:00.000Z");
    }

    @Test
    public void testToWeek() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToWeek func = new ISOToWeek();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-12T00:00:00.000Z");
    }

    @Test
    public void testToDay() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToDay func = new ISOToDay();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T00:00:00.000Z");
    }

    @Test
    public void testToHour() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToHour func = new ISOToHour();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:00:00.000Z");
    }

    @Test
    public void testToMinute() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToMinute func = new ISOToMinute();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:11:00.000Z");
    }

    @Test
    public void testToSecond() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020Z");

        ISOToSecond func = new ISOToSecond();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:11:33.000Z");
    }


    @Test
    public void testToYear_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToYear func = new ISOToYear();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-01-01T00:00:00.000-08:00");
    }

    @Test
    public void testToMonth_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToMonth func = new ISOToMonth();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-01T00:00:00.000-08:00");
    }

    @Test
    public void testToWeek_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToWeek func = new ISOToWeek();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-12T00:00:00.000-08:00");
    }

    @Test
    public void testToDay_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToDay func = new ISOToDay();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T00:00:00.000-08:00");
    }

    @Test
    public void testToHour_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToHour func = new ISOToHour();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:00:00.000-08:00");
    }

    @Test
    public void testToMinute_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToMinute func = new ISOToMinute();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:11:00.000-08:00");
    }

    @Test
    public void testToSecond_Timezone() throws Exception {

        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, "2010-04-15T08:11:33.020-08:00");

        ISOToSecond func = new ISOToSecond();
        String truncated = func.exec(t1);

        assertEquals(truncated, "2010-04-15T08:11:33.000-08:00");
    }

}