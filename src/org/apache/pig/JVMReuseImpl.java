/**
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

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.DistinctCombiner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigCombiner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigGenericMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.pig.tools.pigstats.PigStatusReporter;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JVMReuseImpl {

    private static Log LOG = LogFactory.getLog(JVMReuseImpl.class);

    public void cleanupStaticData() {

        // Calling Pig builtin ones directly without reflection for optimization
        // and to reduce probability of NPE in PIG-4418
        SpillableMemoryManager.staticDataCleanup();
        PhysicalOperator.staticDataCleanup();
        PigContext.staticDataCleanup();
        PigGenericMapReduce.staticDataCleanup();
        PigStatusReporter.staticDataCleanup();
        PigCombiner.Combine.staticDataCleanup();
        DistinctCombiner.Combine.staticDataCleanup();

        String className = null;
        String msg = null;
        List<Method> staticCleanupMethods = JVMReuseManager.getInstance()
                .getStaticDataCleanupMethods();
        for (Method m : staticCleanupMethods) {
            try {
                className = m.getDeclaringClass() == null ? "anonymous" : m.getDeclaringClass().getName();
                msg = "Invoking method " + m.getName() + " in class "
                        + className + " for static data cleanup";
                if (className.startsWith("org.apache.pig")) {
                    LOG.debug(msg);
                } else {
                    LOG.info(msg);
                }
                m.invoke(null);
                msg = null;
            } catch (Exception e) {
                LOG.error("Exception while calling static methods:"
                        + getMethodNames(staticCleanupMethods) + ". " + msg, e);
                throw new RuntimeException("Error while " + msg, e);
            }
        }
    }

    private String getMethodNames(List<Method> staticCleanupMethods) {
        StringBuilder sb = new StringBuilder();
        for (Method m : staticCleanupMethods) {
            if (m == null) {
                sb.append("null,");
            } else {
                sb.append(m.getDeclaringClass() == null ? "anonymous" : m.getDeclaringClass().getName());
                sb.append(".").append(m.getName()).append(",");
            }
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}
