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

package org.apache.pig.builtin;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.concurrent.TimeUnit;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.MonitoredUDFExecutor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.MonitoredUDFExecutor.ErrorCallback;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;


/**
 * Describes how the execution of a UDF should be monitored, and what 
 * to do if it times out.
 * <p>
 * NOTE: does not work with UDFs that implement the Accumulator interface
 * <p>
 *     
 * Setting a default value will cause it to be used instead of null when the UDF times out.
 * The appropriate value among in, long, string, etc, is used.
 * The default fields of these annotations are arrays for Java reasons I won't bore you with.
 * <p>
 * Set them as if they were primitives: <code>@MonitoredUDF( intDefault=5 )</code>
 * <p>
 * There is currently no way to provide a default ByteArray, Tuple, Map, or Bag. Null will always be used for those.
 * <p>
 * Currently this annotation is ignored when the Accumulator interface is used.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@Documented
@Inherited
@Retention(value=RetentionPolicy.RUNTIME)
public @interface MonitoredUDF {
    /**
     * Time Units in which to measure timeout value.
     * @return Time Units in which to measure timeout value.
     */
    TimeUnit timeUnit() default TimeUnit.SECONDS;
    
    /**
     * Number of time units after which the execution should be halted and default returned.
     * @return Number of time units after which the execution should be halted and default returned.
     */
    int duration() default 10;
    
    int[] intDefault() default {};
    long[] longDefault() default {};
    double[] doubleDefault() default {};
    float[] floatDefault() default {};
    String[] stringDefault() default {};
  
    /**
     * UDF author can implement a static extension of MonitoredUDFExecutor.ErrorCallback and provide its class
     * to the annotation in order to perform custom error handling.
     */
    Class<? extends ErrorCallback> errorCallback() default MonitoredUDFExecutor.ErrorCallback.class;
}
