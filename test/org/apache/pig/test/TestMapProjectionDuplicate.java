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

import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestMapProjectionDuplicate {
    private static PigServer pigServer;
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }
    
    @Test
    public void testDuplicate() throws Exception {
        Data data = resetData(pigServer);

        Map<String,String> m1 = ImmutableMap.of("name", "jon", "age", "26");
        
        data.set("foo", Utils.getSchemaFromString("a:[chararray]"), tuple(m1));
        
        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (a:map[chararray]);");
        pigServer.registerQuery("b = foreach @ generate a#'name' as name, a#'age' as age;");
        pigServer.registerQuery("c = foreach @ generate *;");
        Schema s = pigServer.dumpSchema("c");
        assertEquals("name", s.getField(0).alias);
        assertEquals("age", s.getField(1).alias);
    }
}
