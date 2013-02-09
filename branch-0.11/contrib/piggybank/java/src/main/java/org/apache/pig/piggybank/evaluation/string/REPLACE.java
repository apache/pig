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

package org.apache.pig.piggybank.evaluation.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * string.REPLACE implements eval function to replace part of a string.
 * Example:<code>
 *      register pigudfs.jar;
 *      A = load 'mydata' as (name);
 *      B = foreach A generate string.REPLACE(name, 'blabla', 'bla');
 *      dump B;
 *      </code>
 * The first argument is a string on which to perform the operation. The second argument
 * is treated as a regular expression. The third argument is the replacement string.
 * This is a wrapper around Java's String.replaceAll(String, String);
 * 
 */

/**
 * @deprecated Use {@link org.apache.pig.builtin.REPLACE}
 */
@Deprecated 
public class REPLACE extends org.apache.pig.builtin.REPLACE {}