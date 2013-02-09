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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

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
 * When eval functions implement this interface, Pig will attempt to use MapReduce's combiner.
 * The initial funciton will be called in the map phase and be passed a single tuple.  The
 * intermediate function will be called 0 or more times in the combiner phase.  And the final
 * function will be called once in the reduce phase.  It is important that the results be the same
 * whether the intermediate function is called 0, 1, or more times.  Hadoop makes no guarantees
 * about how many times the combiner will be called in a job.
 *
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Algebraic{
    
    /**
     * Get the initial function. 
     * @return A function name of f_init. f_init should be an eval func.
     * The return type of f_init.exec() has to be Tuple
     */
    public String getInitial();

    /**
     * Get the intermediate function. 
     * @return A function name of f_intermed. f_intermed should be an eval func.
     * The return type of f_intermed.exec() has to be Tuple
     */
    public String getIntermed();

    /**
     * Get the final function. 
     * @return A function name of f_final. f_final should be an eval func parametrized by
     * the same datum as the eval func implementing this interface.
     */
    public String getFinal();
}
