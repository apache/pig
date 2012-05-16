/**
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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import static org.apache.pig.builtin.mock.Storage.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.junit.Test;

public class TestJobControlSleep {

  /**
   * The hadoop provided JobControl sleeps 5000 ms in between job status checks
   * making the minimum runtime of a job effectively 5 seconds
   * This tests checks that we don't get back to this behavior
   * @see HadoopShims#newJobControl(String, org.apache.hadoop.conf.Configuration, org.apache.pig.impl.PigContext)
   */
  @Test
  public void testLocalModeTakesLessThan5secs() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Data data = resetData(pigServer);
    data.set("in", tuple("a"));
    long t0 = System.currentTimeMillis();
    pigServer.registerQuery(
        "A = LOAD 'in' using mock.Storage();\n"
        + "STORE A INTO 'out' USING mock.Storage();");
    long t1 = System.currentTimeMillis();
    List<Tuple> list = data.get("out");
    assertEquals(1, list.size());
    assertEquals("a", list.get(0).get(0));
    assertTrue("must take less than 5 seconds", (t1 - t0) < 5000);
  }

}
