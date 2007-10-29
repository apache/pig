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

import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LORead;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;


/**
 * LocalPlanCompliler will general an execution plan for ExecType.LOCAL jobs
 * using the PhysicalOperator framework.  It fulfills as similar role as the 
 * MapreducePlanCompiler, but is far more simple.
 * @author dnm
 *
 */
public class LocalPlanCompiler extends PlanCompiler {

    protected LocalPlanCompiler(PigContext pigContext) {
		super(pigContext);
    }
	@Override
    public PhysicalOperator compile(LogicalOperator lo, Map queryResults) throws IOException {
        PhysicalOperator po = compileOperator(lo, queryResults);
        for (int i = 0; i < lo.getInputs().size(); i++) {
            po.inputs[i] = compile(lo.getInputs().get(i), queryResults);
        }
        return po;
    }

    protected PhysicalOperator compileOperator(LogicalOperator lo, Map queryResults) throws IOException {

        if (lo instanceof LOEval) {
            return new POEval(((LOEval) lo).getSpec(),lo.getOutputType());
        } else if (lo instanceof LOCogroup) {
            return new POCogroup(((LOCogroup) lo).getSpecs(),lo.getOutputType());
        }  else if (lo instanceof LOLoad) {
            LOLoad lol = (LOLoad) lo;
            
            return new POLoad(pigContext, lol.getInputFileSpec(),lo.getOutputType());

        } else if (lo instanceof LORead) {
            IntermedResult readFrom = ((LORead) lo).getReadFrom();

            if (readFrom.executed()) {
                // reading from a materialized databag; use PORead
                return new PORead(readFrom.read(),readFrom.getOutputType());
            } else {
                if (readFrom.compiled()) {
                    // other plan already compiled, so split its output
                    return new POSplitSlave(readFrom.pp.root,readFrom.getOutputType());
                } else {
                    // other plan not compiled yet, so compile it and use it directly
                    readFrom.compile(queryResults);
                    return readFrom.pp.root;
                }
            }
        } else if (lo instanceof LOStore) {
            LOStore los = (LOStore) lo;
            return new POStore(los.getOutputFileSpec(),los.isAppend(),pigContext);
        } else if (lo instanceof LOUnion) {
            return new POUnion(((LOUnion) lo).getInputs().size(),lo.getOutputType());
        } else if (lo instanceof LOSort) {
        	return new POSort(((LOSort)lo).getSortSpec(), lo.getOutputType());
        }
        else {
            throw new IOException("Internal error: Unknown logical operator.");
        }
    }

}
