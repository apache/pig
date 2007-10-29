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
package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LogicalPlan;


public class PhysicalPlan {
    public PhysicalOperator root;

    public PhysicalPlan(PhysicalOperator rootIn) {
        root = rootIn;
    }

    public DataBag exec(boolean continueFromLast) throws IOException {
        DataBag results = new DataBag();

        root.open(continueFromLast);
        Tuple t;
        while ((t = (Tuple) root.getNext()) != null)
            results.add(t);
        root.close();

        return results;
    }

    // this constructor compiles a logical plan into a (local or mapreduce or hadoop) physical plan:
    public PhysicalPlan(LogicalPlan lp, Map queryResults) throws IOException {
    	switch(lp.getPigContext().getExecType()) {
    	case LOCAL:
            // always append a splitmaster on the front of each newly-compiled plan
            // (allows future plans to read from it)
            root = new POSplitMaster(new LocalPlanCompiler(lp.getPigContext()).compile(lp.getRoot(), queryResults),lp.getOutputType());
            break;
    	case MAPREDUCE:
            root = (new MapreducePlanCompiler(lp.getPigContext())).compile(lp.getRoot(), queryResults);
            break;
        default:
            throw new IOException("Unknown execType.");
        }
    }
}
