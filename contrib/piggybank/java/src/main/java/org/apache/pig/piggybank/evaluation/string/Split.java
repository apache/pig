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

import java.util.Arrays;
import java.util.regex.PatternSyntaxException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.builtin.STRSPLIT;

/**
 * Wrapper around Java's String.split<br>
 * input tuple: first column is assumed to have a string to split;<br>
 * the optional second column is assumed to have the delimiter or regex to split on;<br>
 * if not provided, it's assumed to be '\s' (space)<br>
 * the optional third column may provide a limit to the number of results.<br>
 * If limit is not provided, 0 is assumed, as per Java's split().
 */

/**
 * @deprecated Use {@link org.apache.pig.builtin.STRSPLIT}
 */
@Deprecated 

public class Split extends STRSPLIT {}
