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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.ByteArrayOutputStream;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

import com.google.common.collect.Sets;

/**
 * An operator model for a Tez job. Acts as a host to the plans that will
 * execute in Tez vertices.
 */
public class TezOperator extends Operator<TezOpPlanVisitor> {
    private static final long serialVersionUID = 1L;

    public PhysicalPlan plan;
    public PhysicalPlan combinePlan;

    public Set<String> UDFs;
    public Set<PhysicalOperator> scalars;

    // TODO: We need to specify parallelism per vertex in Tez. For now, we set
    // them all to 1.
    int requestedParallelism = 1;

    // TODO: When constructing Tez vertex, we have to specify how much resource
    // the vertex will need. So we need to estimate these values while compiling
    // physical plan into tez plan. For now, we're using default values - 1G mem
    // and 1 core.
    int requestedMemory = 1024;
    int requestedCpu = 1;

    // Name of the Custom Partitioner used
    String customPartitioner = null;

    // Indicates that the plan creation is complete
    boolean closed = false;

    // Types of blocking operators. For now, we only support the following ones.
    private static enum OPER_FEATURE {
        NONE,
        // Indicate if this job is a group by job
        GROUPBY,
        // Indicate if this job is a cogroup job
        COGROUP,
        // Indicate if this job is a regular join job
        HASHJOIN;
    };

    OPER_FEATURE feature = OPER_FEATURE.NONE;

    public TezOperator(OperatorKey k) {
        super(k);
        plan = new PhysicalPlan();
        combinePlan = new PhysicalPlan();
        UDFs = Sets.newHashSet();
        scalars = Sets.newHashSet();
    }

    public String getProcessorName() {
        return PigProcessor.class.getName();
    }

    @Override
    public void visit(TezOpPlanVisitor v) throws VisitorException {
        v.visitTezOp(this);
    }

    public boolean supportsMultipleInputs() {
        return true;
    }

    public boolean supportsMultipleOutputs() {
        return true;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public boolean isGroupBy() {
        return (feature == OPER_FEATURE.GROUPBY);
    }

    public void markGroupBy() {
        feature = OPER_FEATURE.GROUPBY;
    }

    public boolean isCogroup() {
        return (feature == OPER_FEATURE.COGROUP);
    }

    public void markCogroup() {
        feature = OPER_FEATURE.COGROUP;
    }

    public boolean isRegularJoin() {
        return (feature == OPER_FEATURE.HASHJOIN);
    }

    public void markRegularJoin() {
        feature = OPER_FEATURE.HASHJOIN;
    }

    @Override
    public String name() {
        String udfStr = getUDFsAsStr();
        StringBuilder sb = new StringBuilder("Tez" + "(" + requestedParallelism +
                (udfStr.equals("")? "" : ",") + udfStr + ")" + " - " + mKey.toString());
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name() + ":\n");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (!plan.isEmpty()) {
            plan.explain(baos);
            String mp = new String(baos.toByteArray());
            sb.append(shiftStringByTabs(mp, "|   "));
        } else {
            sb.append("Plan Empty");
        }
        return sb.toString();
    }

    private String getUDFsAsStr() {
        StringBuilder sb = new StringBuilder();
        if(UDFs!=null && UDFs.size()>0){
            for (String str : UDFs) {
                sb.append(str.substring(str.lastIndexOf('.')+1));
                sb.append(',');
            }
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    private String shiftStringByTabs(String DFStr, String tab) {
        StringBuilder sb = new StringBuilder();
        String[] spl = DFStr.split("\n");
        for (int i = 0; i < spl.length; i++) {
            sb.append(tab);
            sb.append(spl[i]);
            sb.append("\n");
        }
        sb.delete(sb.length() - "\n".length(), sb.length());
        return sb.toString();
    }
}

