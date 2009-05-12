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
package org.apache.pig;

/**
 * An interface to declare that an EvalFunc's 
 * calculation can be decomposed into intitial, intermediate, and final steps.
 * More formally, suppose we have to compute an function f over a bag X. In general, we need to know the entire X
 * before we can make any progress on f. However, some functions are <i>algebraic</i> e.g. SUM. In
 * these cases, you can apply some initital function f_init on subsets of X to get partial results. 
 * You can then combine partial results from different subsets of X using an intermediate function
 * f_intermed. To get the final answers, several partial results can be combined by invoking a final
 * f_final function. For the function SUM, f_init, f_intermed, and f_final are all SUM. 
 * 
 * See the code for builtin AVG to get a better idea of how algebraic works.
 * 
 * When eval functions implement this interface, it is a hint to the system to try and compute
 * partial results early which causes queries to run faster. 
 *
 */
public interface Algebraic{
    
    /**
     * 
     * @return A string to instatiate f_init. f_init should be an eval func 
     */
    public String getInitial();

    /**
     * 
     * @return A string to instantiate f_intermed. f_intermed should be an eval func
     */
    public String getIntermed();

    /**
     * 
     * @return A string to instantiate f_final. f_final should be an eval func parametrized by
     * the same datum as the eval func implementing this interface
     */
    public String getFinal();
}
