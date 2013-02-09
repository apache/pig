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

package org.apache.pig.piggybank.test.storage;


import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHadoopJobHistoryLoader {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    private static final String INPUT_DIR = 
        "src/test/java/org/apache/pig/piggybank/test/data/jh";
    
    @SuppressWarnings("unchecked")
    @Test
    public void testHadoopJHLoader() throws Exception {
        PigServer pig = new PigServer(ExecType.LOCAL);
        pig.registerQuery("a = load '" + INPUT_DIR 
                + "' using org.apache.pig.piggybank.storage.HadoopJobHistoryLoader() " 
                + "as (j:map[], m:map[], r:map[]);");
        Iterator<Tuple> iter = pig.openIterator("a");
        
        assertTrue(iter.hasNext());
        
        Tuple t = iter.next();
        
        Map<String, Object> job = (Map<String, Object>)t.get(0);
        
        assertEquals("3eb62180-5473-4301-aa22-467bd685d466", (String)job.get("PIG_SCRIPT_ID"));
        assertEquals("job_201004271216_9998", (String)job.get("JOBID"));
        assertEquals("job_201004271216_9995", (String)job.get("PIG_JOB_PARENTS"));
        assertEquals("0.8.0-dev", (String)job.get("PIG_VERSION"));
        assertEquals("0.20.2", (String)job.get("HADOOP_VERSION"));
        assertEquals("d", (String)job.get("PIG_JOB_ALIAS"));
        assertEquals("PigLatin:Test.pig", job.get("JOBNAME"));
        assertEquals("ORDER_BY", (String)job.get("PIG_JOB_FEATURE"));
        assertEquals("1", (String)job.get("TOTAL_MAPS"));
        assertEquals("1", (String)job.get("TOTAL_REDUCES"));              
    }
}
