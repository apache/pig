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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.builtin.GFCross;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Test;

public class TestGFCross {

    // Test GFCross returns the correct number of default
    // join groups
    @Test
    public void testDefault() throws Exception {
        UDFContext.getUDFContext().addJobConf(null);
        Tuple t = TupleFactory.getInstance().newTuple(2);

        t.set(0, 2);
        t.set(1, 0);

        GFCross cross = new GFCross("1");
        DataBag bag = cross.exec(t);
        assertEquals(10, bag.size());
    }

    // Test GFCross handles the parallel 1 case.
    @Test
    public void testSerial() throws Exception {
        Configuration cfg = new Configuration();
        cfg.set(PigImplConstants.PIG_CROSS_PARALLELISM + ".1", "1");
        cfg.set(MRConfiguration.TASK_ID, "task_1473802673416_1808_m_000000");
        UDFContext.getUDFContext().addJobConf(cfg);
        Tuple t = TupleFactory.getInstance().newTuple(2);

        t.set(0, 2);
        t.set(1, 0);

        GFCross cross = new GFCross("1");
        DataBag bag = cross.exec(t);
        assertEquals(1, bag.size());
    }

    // Test GFCross handles a different parallel  case.
    @Test
    public void testParallelSet() throws Exception {
        Configuration cfg = new Configuration();
        cfg.set(PigImplConstants.PIG_CROSS_PARALLELISM + ".1", "10");
        cfg.set(MRConfiguration.TASK_ID, "task_14738102975522_0001_r_000000");
        UDFContext.getUDFContext().addJobConf(cfg);
        Tuple t = TupleFactory.getInstance().newTuple(2);

        t.set(0, 2);
        t.set(1, 0);

        GFCross cross = new GFCross("1");
        DataBag bag = cross.exec(t);
        assertEquals(4, bag.size());
    }
}
