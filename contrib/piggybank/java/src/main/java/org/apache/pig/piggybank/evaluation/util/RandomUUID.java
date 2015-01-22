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

import java.io.IOException;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * RandomUUID implements eval function to generate a
 * to retrieve a type 4 (pseudo randomly generated) UUID.
 * Usages:
 *  - Generating tokens.
 * Example:
 *      register pigudfs.jar;
 *      A = load 'mydata' as (name);
 *      B = foreach A generate name, RandomUUID();
 *      dump B;
 */
public class RandomUUID extends EvalFunc<String> {

  /**
   * Generate an UUID.
   * @param arg0 not used.
   * @exception IOException
   */
  @Override
  public String exec(Tuple arg0) throws IOException {
    return UUID.randomUUID().toString();
  }
	
  /**
   * This precise the schema of the UDF.
   * @param input Schema of the input data.
   * @return Schema of the output data.
   */
  @Override
  public Schema outputSchema(Schema input) {
    return new Schema(new Schema.FieldSchema("uuid", DataType.CHARARRAY));
  }
}

