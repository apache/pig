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
package org.apache.pig.builtin.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.schema;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.map;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestMockStorage {

  @Test
  public void testMockStoreAndLoad() throws Exception {
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    Data data = resetData(pigServer);

    data.set("foo",
        tuple("a"),
        tuple("b"),
        tuple("c"),
        tuple(map("d","e", "f","g")),
        tuple(bag(tuple("h"),tuple("i")))
        );

    pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
    pigServer.registerQuery("STORE A INTO 'bar' USING mock.Storage();");

    List<Tuple> out = data.get("bar");
    assertEquals(tuple("a"), out.get(0));
    assertEquals(tuple("b"), out.get(1));
    assertEquals(tuple("c"), out.get(2));
    assertEquals(tuple(map("f", "g", "d", "e" )), out.get(3));
    assertEquals(tuple(bag(tuple("h"),tuple("i"))), out.get(4));
  }

  @Test
  public void testMockSchema() throws Exception {
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    Data data = resetData(pigServer);

    data.set("foo", "blah:chararray",
        tuple("a"),
        tuple("b"),
        tuple("c")
        );

    pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
    pigServer.registerQuery("B = FOREACH A GENERATE blah as a, blah as b;");
    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

    assertEquals(schema("a:chararray,b:chararray"), data.getSchema("bar"));

    List<Tuple> out = data.get("bar");
    assertEquals(tuple("a", "a"), out.get(0));
    assertEquals(tuple("b", "b"), out.get(1));
    assertEquals(tuple("c", "c"), out.get(2));
  }

  @Test
  public void testMockStoreUnion() throws Exception {
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    Data data = resetData(pigServer);

    data.set("input1",
        tuple("a"),
        tuple("b"),
        tuple("c")
        );

    data.set("input2",
            tuple("d"),
            tuple("e"),
            tuple("f")
            );

    pigServer.registerQuery("A = LOAD 'input1' USING mock.Storage();");
    pigServer.registerQuery("B = LOAD 'input2' USING mock.Storage();");
    pigServer.registerQuery("C = UNION A, B;");
    pigServer.registerQuery("STORE C INTO 'output' USING mock.Storage();");

    List<Tuple> out = data.get("output");
    assertEquals(out + " size", 6, out.size());
    Set<String> set = new HashSet<String>();
    for (Tuple tuple : out) {
        if (!set.add((String)tuple.get(0))) {
            fail(tuple.get(0) + " is present twice in " + out);
        }
    }

    assertTrue(set + " contains a", set.contains("a"));
    assertTrue(set + " contains b", set.contains("b"));
    assertTrue(set + " contains c", set.contains("c"));
    assertTrue(set + " contains d", set.contains("d"));
    assertTrue(set + " contains e", set.contains("e"));
    assertTrue(set + " contains f", set.contains("f"));
  }
  
  @Test
  public void testBadUsage1() throws Exception {
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    Data data = resetData(pigServer);

    data.set("input1",
            tuple("a"),
            tuple("b"),
            tuple("c")
            );

    try {
        data.set("input1",
                tuple("d"),
                tuple("e"),
                tuple("f")
                );
        fail("should have thrown an exception for setting twice the same input");
    } catch (RuntimeException e) {
        assertEquals("Can not set location input1 twice", e.getMessage());
    }
  }
  
  @Test
  public void testBadUsage2() throws Exception {
    PigServer pigServer = new PigServer(Util.getLocalTestMode());
    Data data = resetData(pigServer);

    data.set("input",
        tuple("a"),
        tuple("b"),
        tuple("c")
        );

    pigServer.setBatchOn();
    pigServer.registerQuery(
         "A = LOAD 'input' USING mock.Storage();"
        +"B = LOAD 'input' USING mock.Storage();"
        +"STORE A INTO 'output' USING mock.Storage();"
        +"STORE B INTO 'output' USING mock.Storage();");
    List<ExecJob> results = pigServer.executeBatch();
    boolean failed = false;
    for (ExecJob execJob : results) {
        if (execJob.getStatus() == JOB_STATUS.FAILED) {
            failed = true;
            break;
        }
    }
    if (Util.getLocalTestMode().toString().equals("TEZ_LOCAL")) {
        assertFalse("job should have success in tez_local mode since we have two vertex, each for a single store", failed);
        return;
    }
    assertTrue("job should have failed for storing twice in the same location", failed);

  }
}
