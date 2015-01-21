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
package org.apache.pig.piggybank.evaluation.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.junit.Before;
import org.junit.Test;

/**
 * Test class for RandomUUID UDF.
 */ 
public class TestRandomUUID {

  /**
   * Test that the output schema name is 'uuid'.
   */
  @Test
  public void testSchema() throws Exception {
    RandomUUID func = new RandomUUID();
    Schema in = Schema.generateNestedSchema(DataType.TUPLE);
    Schema out = func.outputSchema(in);
    assertEquals("{uuid: chararray}", out.toString());
  }
  
  /**
   * Test that the generated UUID match the specified pattern.
   */
  @Test
  public void testUUIDFormat() throws Exception {
    RandomUUID func = new RandomUUID();
    Tuple t = TupleFactory.getInstance().newTuple(0);
    String uuid = func.exec(t);
    assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
  }
}
