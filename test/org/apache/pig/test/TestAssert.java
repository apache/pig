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

import static junit.framework.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Test;

public class TestAssert {

  /**
   * Verify that ASSERT operator works
   * @throws Exception
   */
  @Test
  public void testPositive() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      pigServer.setBatchOn();
      pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
      pigServer.registerQuery("ASSERT A BY i > 0;");
      pigServer.registerQuery("STORE A INTO 'bar' USING mock.Storage();");

      pigServer.executeBatch();

      List<Tuple> out = data.get("bar");
      assertEquals(3, out.size());
      assertEquals(tuple(1), out.get(0));
      assertEquals(tuple(2), out.get(1));
      assertEquals(tuple(3), out.get(2));
  }

  /**
   * Verify that ASSERT operator works in a Pig script
   * See PIG-3670
   * @throws Exception
   */
  @Test
  public void testInScript() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      StringBuffer query = new StringBuffer();
      query.append("A = LOAD 'foo' USING mock.Storage() AS (i:int);\n");
      query.append("ASSERT A BY i > 0;\n");
      query.append("STORE A INTO 'bar' USING mock.Storage();");

      InputStream is = new ByteArrayInputStream(query.toString().getBytes());
      pigServer.registerScript(is);

      List<Tuple> out = data.get("bar");
      assertEquals(3, out.size());
      assertEquals(tuple(1), out.get(0));
      assertEquals(tuple(2), out.get(1));
      assertEquals(tuple(3), out.get(2));
  }

  /**
   * Verify that ASSERT operator works
   * @throws Exception
   */
  @Test
  public void testNegative() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
      pigServer.registerQuery("ASSERT A BY i > 1 , 'i should be greater than 1';");
      
      try {
          pigServer.openIterator("A");
      } catch (FrontendException fe) {
            Assert.assertTrue(fe.getCause().getCause().getMessage().contains("Assertion violated"));
      }
  }

  /**
   * Verify that ASSERT operator works. Disable fetch for this testcase.
   * @throws Exception
   */
  @Test
  public void testNegativeWithoutFetch() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );

      pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
      pigServer.registerQuery("ASSERT A BY i > 1 , 'i should be greater than 1';");

      Properties props = pigServer.getPigContext().getProperties();
      props.setProperty(PigConfiguration.OPT_FETCH, "false");
      try {
          pigServer.openIterator("A");
      } catch (FrontendException fe) {
          Assert.assertTrue(fe.getCause().getMessage().contains(
                  "Job terminated with anomalous status FAILED"));
      }
  }
  
  /**
   * Verify that alias is not assignable to the ASSERT operator
   * @throws Exception
   */
  @Test(expected=FrontendException.class)
  public void testNegativeWithAlias() throws Exception {
      PigServer pigServer = new PigServer(ExecType.LOCAL);
      Data data = resetData(pigServer);

      data.set("foo",
              tuple(1),
              tuple(2),
              tuple(3)
              );
      try {
          pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
          pigServer.registerQuery("B = ASSERT A BY i > 1 , 'i should be greater than 1';");
      }
      catch (FrontendException fe) {
          Util.checkMessageInException(fe, "Syntax error, unexpected symbol at or near 'B'");
          throw fe;
      }
  }

}
