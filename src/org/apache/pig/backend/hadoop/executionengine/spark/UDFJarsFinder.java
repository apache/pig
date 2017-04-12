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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;

//find udf jars which will be downloaded with spark job on every nodes
public class UDFJarsFinder extends SparkOpPlanVisitor {
    private PigContext pigContext = null;
    private Set<String> udfJars = new HashSet();

    public UDFJarsFinder(SparkOperPlan plan, PigContext pigContext) {
        super(plan, new DependencyOrderWalker(plan));
        this.pigContext = pigContext;
    }

    public void visitSparkOp(SparkOperator sparkOp)
            throws VisitorException {
        for (String udf : sparkOp.UDFs) {
            try {
                Class clazz = this.pigContext.getClassForAlias(udf);
                if (clazz != null) {
                    String jar = JarManager.findContainingJar(clazz);
                    if (jar != null) {
                        this.udfJars.add(jar);
                    }
                }
            } catch (IOException e) {
                throw new VisitorException(e);
            }
        }
    }

    public Set<String> getUdfJars() {
        return this.udfJars;
    }
}
