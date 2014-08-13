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
package org.apache.pig.piggybank.test.evaluation.datetime.convert;

import static org.junit.Assert.*;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO;
import org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix;
import org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO;
import org.junit.Test;

public class TestConvertDateTime {

    @Test
    public void testBadFormat() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, "2011-01-01");
        t1.set(1, "MMMM, yyyy");
        CustomFormatToISO convert = new CustomFormatToISO();
        assertNull("Input that doesn't match format should result in null", convert.exec(t1));
        t1.set(0, "July, 2012");
        assertEquals("Matching format should work correctly", "2012-07-01T00:00:00.000Z", convert.exec(t1));
    }

    @Test
    public void testUnixToISO() throws Exception {

        // Verify that (long) unix datetimes convert to ISO datetimes
        Tuple t1 = TupleFactory.getInstance().newTuple(1);
        t1.set(0, 1231290421000L);

        UnixToISO func = new UnixToISO();
        String iso = func.exec(t1);

        assertTrue(iso.equals("2009-01-07T01:07:01.000Z"));
    }

    @Test
    public void testISOToUnix() throws Exception {

        // Verify that ISO string datetimes convert to Unix (long) datetimes
        Tuple t2 = TupleFactory.getInstance().newTuple(1);
        t2.set(0, "2009-01-07T01:07:01.000Z");
        ISOToUnix func2 = new ISOToUnix();
        Long unix = func2.exec(t2);

        assertTrue(unix == 1231290421000L);

    }

    @Test
    public void testCustomFormatToISO() throws Exception {

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "10/10/2010");
        t.set(1, "dd/MM/yyyy");
        CustomFormatToISO func = new CustomFormatToISO();
        String iso = func.exec(t);

        assertTrue(iso.equals("2010-10-10T00:00:00.000Z"));
    }
}
