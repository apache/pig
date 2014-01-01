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
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An operator model for a Tez job. Acts as a host to the plans that will
 * execute in Tez vertices.
 */
public class TezOperator extends Operator<TezOpPlanVisitor> {
    private static final long serialVersionUID = 1L;

    // Processor pipeline
    public PhysicalPlan plan;

    // Descriptors for out-bound edges.
    public Map<OperatorKey, TezEdgeDescriptor> outEdges;
    // Descriptors for in-bound edges.
    public Map<OperatorKey, TezEdgeDescriptor> inEdges;

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

    // Presence indicates that this TezOper is sub-plan of a POSplit.
    // Only POStore or POLocalRearrange leaf can be a sub-plan of POSplit
    private OperatorKey splitOperatorKey = null;

    // Indicates that the plan creation is complete
    boolean closed = false;

    // Indicate whether we need to split the DAG below the operator
    // The result is two or more DAG connected DAG inside the same plan container
    boolean segmentBelow = false;

    // Indicates if this is a limit after a sort
    boolean limitAfterSort = false;

    //Indicates if this job is an order by job
    boolean globalSort = false;

    //The quantiles file name if globalSort is true
    String quantFile;

    //The sort order of the columns;
    //asc is true and desc is false
    boolean[] sortOrder;

    // Last POLimit value in this map reduce operator, needed by LimitAdjuster
    // to add additional map reduce operator with 1 reducer after this
    long limit = -1;
    
    // If not null, need to collect sample sent from predecessor
    TezOperator sampleOperator = null;

    // Types of blocking operators. For now, we only support the following ones.
    private static enum OPER_FEATURE {
        NONE,
        // Indicate if this job is a union job
        UNION,
        // Indicate if this job is a sampling job
        SAMPLER,
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
        outEdges = Maps.newHashMap();
        inEdges = Maps.newHashMap();
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

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    public OperatorKey getSplitOperatorKey() {
        return splitOperatorKey;
    }

    public void setSplitOperatorKey(OperatorKey splitOperatorKey) {
        this.splitOperatorKey = splitOperatorKey;
    }

    public boolean isSplitSubPlan() {
        return splitOperatorKey != null;
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

    public boolean isUnion() {
        return (feature == OPER_FEATURE.UNION);
    }

    public void markUnion() {
        feature = OPER_FEATURE.UNION;
    }

    public boolean isSampler() {
        return (feature == OPER_FEATURE.SAMPLER);
    }

    public void markSampler() {
        feature = OPER_FEATURE.SAMPLER;
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

    public boolean needSegmentBelow() {
        return segmentBelow;
    }

    public String getQuantFile() {
        return quantFile;
    }

    public void setQuantFile(String quantFile) {
        this.quantFile = quantFile;
    }

    public void setSortOrder(boolean[] sortOrder) {
        if(null == sortOrder) return;
        this.sortOrder = new boolean[sortOrder.length];
        for(int i = 0; i < sortOrder.length; ++i) {
            this.sortOrder[i] = sortOrder[i];
        }
    }

    public boolean[] getSortOrder() {
        return sortOrder;
    }

    public void setGlobalSort(boolean globalSort) {
        this.globalSort = globalSort;
    }

    public boolean isGlobalSort() {
        return globalSort;
    }

    public boolean isLimitAfterSort() {
        return limitAfterSort;
    }
}

