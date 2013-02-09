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

package org.apache.pig.piggybank.evaluation.math;

/**
 * math.ACOS implements a binding to the Java function
* {@link java.lang.Math#acos(double) Math.acos(double)} for computing the
* arc cosine of value of the argument. The returned value will be a double which is 
* the arc cosine of the value of  input.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Double</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>Double</code> arc cosine of the value of input</dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>acos_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ACOS(float1);
* </code></dd>
* </dl>
* 
* @see Math#acos(double)
* @see
* @author ajay garg
*
*/

/**
 * @deprecated Use {@link org.apache.pig.builtin.ACOS}
 */
@Deprecated 

public class ACOS extends DoubleBase{
	
	Double compute(Double input){
		return Math.acos(input);
		
	}
}
