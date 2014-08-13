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
 * math.ATAN2 implements a binding to the Java function
* {@link java.lang.Math#atan2(double,double) Math.atan2(double,double)}. Given a tuple with
* two data atom x and y it will returns the angle theta from the conversion of 
* rectangular coordinates (x, y) to polar coordinates (r, theta).
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two Double</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>Double</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>atan2_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ATAN2(float1);
* </code></dd>
* </dl>
* 
* @see Math#atan2(double,double)
* @see
* @author ajay garg
*
*/
public class ATAN2 extends DoubleDoubleBase{
	Double compute(Double input1, Double input2){
	    return Math.atan2(input1, input2);	
	}
}
