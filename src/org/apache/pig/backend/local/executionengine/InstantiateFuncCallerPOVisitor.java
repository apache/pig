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

/**
 * 
 */
package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.POMapreduce;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POVisitor;

/**
 * POVisitor calling instantiateFunc on all EvalSpec members of visited nodes.
 */
public class InstantiateFuncCallerPOVisitor extends POVisitor {

    private FunctionInstantiator instantiator;

    protected InstantiateFuncCallerPOVisitor(FunctionInstantiator instantiator,
            Map<OperatorKey, ExecPhysicalOperator> opTable) {
        super(opTable);
        this.instantiator = instantiator;
    }

    private void callInstantiateFunc(EvalSpec spec) {
        try {
            spec.instantiateFunc(instantiator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void visitCogroup(POCogroup g) {
        super.visitCogroup(g);
        for (EvalSpec es : g.specs)
            callInstantiateFunc(es);
    }

    @Override
    public void visitMapreduce(POMapreduce mr) {
        super.visitMapreduce(mr);
        for (EvalSpec es : mr.groupFuncs)
            callInstantiateFunc(es);
    }

    @Override
    public void visitSort(POSort s) {
        super.visitSort(s);
        callInstantiateFunc(s.sortSpec);
    }

    @Override
    public void visitEval(POEval e) {
        super.visitEval(e);
        callInstantiateFunc(e.spec);
    }
}
