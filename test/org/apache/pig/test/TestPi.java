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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Testcase aimed at testing pig with large file sizes and filter and group functions
 */
public class TestPi {

    private final Log log = LogFactory.getLog(getClass());

    private long defaultBlockSize = (new Configuration()).getLong("dfs.block.size", 0);

    private long total = ((defaultBlockSize >> 20) / 10) << 20;
    private int inCircle = 0;

    private long totalLength = 0, totalLengthTest = 0;

    PigServer pig;
    String fileName, tmpFile1;

    @Before
    public void setUp() throws Exception{

        log.info("Generating test data...");
        log.info("Default block size = " + defaultBlockSize);
        log.info("Total no. of iterations to run for test data = " + total);

        Random rand = new Random();

        pig = new PigServer(ExecType.LOCAL);
        Data data = resetData(pig);
        List<Tuple> theInput = Lists.newArrayList();

        for(int i = 0; i < total; i++) {
            Double x = new Double(rand.nextDouble());
            Double y = new Double(rand.nextDouble());
            double x1 = x.doubleValue() - 0.5;
            double y1 = y.doubleValue() - 0.5;
            double sq_dist = (x1*x1) + (y1*y1);
            if(sq_dist <= 0.25) {
                inCircle ++;

            }

            theInput.add(tuple(x, y));

            totalLength += String.valueOf(sq_dist).length();
        }
        data.set("foo", theInput);
    }

    @Test
    public void testPi() throws Exception {

        pig.registerQuery("A = load 'foo' using mock.Storage() as (x:double, y:double);");
        pig.registerQuery("B = foreach A generate $0 - 0.5 as d1, $1 - 0.5 as d2;");
        pig.registerQuery("C = foreach B generate $0 * $0 as m1, $1 * $1 as m2;");
        pig.registerQuery("D = foreach C generate $0 + $1 as s1;");
        pig.registerQuery("D = foreach D generate $0, ARITY($0);");

        pig.registerQuery("E = filter D by $0 <= 0.25;");

        pig.registerQuery("F = cogroup D by $1, E by $1;");
        pig.registerQuery("G = foreach F generate COUNT($1) as total, COUNT($2) as in_circle;");

        Iterator<Tuple> values = pig.openIterator("G");
        assertTrue(values.hasNext());
        Tuple finalValues = values.next();
        int totalPoints = DataType.toInteger(finalValues.get(0));
        int inCirclePoints = DataType.toInteger(finalValues.get(1));

        log.info("Value of PI = " + 4 * (double)inCircle / (double)total);
        log.info("Value of PI (From Test data) = " + 4 * (double)inCirclePoints / (double)totalPoints);


        Iterator<Tuple> lengthTest = pig.openIterator("D");

        while(lengthTest.hasNext()) {
            Tuple temp = lengthTest.next();
            totalLengthTest += temp.get(0).toString().length();
        }

        assertEquals(totalPoints, total);
        assertEquals(inCirclePoints, inCircle);
        assertEquals(totalLengthTest, totalLength);
    }
}