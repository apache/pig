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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.VertexGroup;

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

    // Use AtomicInteger for access by reference and being able to reset in
    // TezDAGBuilder based on number of input splits.
    // We just need mutability and not concurrency
    // This is to ensure that vertexes with 1-1 edge have same parallelism
    // even when parallelism of source vertex changes.
    // Can change to int and set to -1 if TEZ-800 gets fixed.
    private AtomicInteger requestedParallelism = new AtomicInteger(-1);
    
    private int estimatedParallelism = -1;

    // TODO: When constructing Tez vertex, we have to specify how much resource
    // the vertex will need. So we need to estimate these values while compiling
    // physical plan into tez plan. For now, we're using default values - 1G mem
    // and 1 core.
    //int requestedMemory = 1024;
    //int requestedCpu = 1;

    // This indicates that this TezOper is a split operator
    private boolean splitter;

    // This indicates that this TezOper has POSplit as a predecessor.
    private OperatorKey splitParent = null;

    // Indicates that the plan creation is complete
    boolean closed = false;

    // Indicate whether we need to split the DAG below the operator
    // The result is two or more DAG connected DAG inside the same plan container
    boolean segmentBelow = false;

    // Indicates if this is a limit after a sort
    boolean limitAfterSort = false;

    //Indicates if this job is an order by job
    boolean globalSort = false;

    //Indicate if this job is a union job 
    boolean union = false;

    //The sort order of the columns;
    //asc is true and desc is false
    boolean[] sortOrder;

    // Last POLimit value in this map reduce operator, needed by LimitAdjuster
    // to add additional map reduce operator with 1 reducer after this
    long limit = -1;

    private boolean skewedJoin = false;

    // Flag to indicate if the small input splits need to be combined to form a larger
    // one in order to reduce the number of mappers. For merge join, both tables
    // are NOT combinable for correctness.
    private boolean combineSmallSplits = true;

    // Used by partition vertex, if not null, need to collect sample sent from predecessor
    TezOperator sampleOperator = null;

    // Used by sample vertex, send parallelism event to orderOperator
    TezOperator sortOperator = null;

    // If the flag is set, FindQuantilesTez/PartitionSkewedKeysTez will use aggregated sample
    // to calculate the number of parallelism at runtime, instead of the numQuantiles/totalReducers_
    // parameter set statically
    private boolean needEstimateParallelism = false;

    // If true, we will use secondary key sort in the job
    private boolean useSecondaryKey = false;

    // Types of blocking operators. For now, we only support the following ones.
    private static enum OPER_FEATURE {
        NONE,
        // Indicate if this job is a merge indexer
        INDEXER,
        // Indicate if this job is a sampling job
        SAMPLER,
        // Indicate if this job is a sample aggregation job
        SAMPLE_AGGREGATOR,
        // Indicate if this job is a sample based partition job (order by/skewed join)
        SAMPLE_BASED_PARTITIONER,
        // Indicate if this job is a group by job
        GROUPBY,
        // Indicate if this job is a cogroup job
        COGROUP,
        // Indicate if this job is a regular join job
        HASHJOIN;
    };

    OPER_FEATURE feature = OPER_FEATURE.NONE;

    private List<OperatorKey> vertexGroupMembers;
    // For union
    private VertexGroupInfo vertexGroupInfo;
    // Mapping of OperatorKey of POStore OperatorKey to vertexGroup TezOperator
    private Map<OperatorKey, OperatorKey> vertexGroupStores = null;

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

    public int getRequestedParallelism() {
        return requestedParallelism.get();
    }

    public void setRequestedParallelism(int requestedParallelism) {
        this.requestedParallelism.set(requestedParallelism);
    }

    public void setRequestedParallelismByReference(TezOperator oper) {
        this.requestedParallelism = oper.requestedParallelism;
    }

    public int getEstimatedParallelism() {
        return estimatedParallelism;
    }

    public void setEstimatedParallelism(int estimatedParallelism) {
        this.estimatedParallelism = estimatedParallelism;
    }

    public int getEffectiveParallelism() {
        return getRequestedParallelism()!=-1? getRequestedParallelism() : getEstimatedParallelism();
    }

    public OperatorKey getSplitParent() {
        return splitParent;
    }

    public void setSplitParent(OperatorKey splitParent) {
        this.splitParent = splitParent;
    }

    public void setSplitter(boolean spl) {
        splitter = spl;
    }

    public boolean isSplitter() {
        return splitter;
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
        return union;
    }

    public void setUnion() {
        union = true;
    }

    public boolean isIndexer() {
        return (feature == OPER_FEATURE.INDEXER);
    }

    public void markIndexer() {
        feature = OPER_FEATURE.INDEXER;
    }

    public boolean isSampler() {
        return (feature == OPER_FEATURE.SAMPLER);
    }

    public void markSampler() {
        feature = OPER_FEATURE.SAMPLER;
    }

    public boolean isSampleAggregation() {
        return (feature == OPER_FEATURE.SAMPLE_AGGREGATOR);
    }

    public void markSampleAggregation() {
        feature = OPER_FEATURE.SAMPLE_AGGREGATOR;
    }
    
    public boolean isSampleBasedPartitioner() {
        return (feature == OPER_FEATURE.SAMPLE_BASED_PARTITIONER);
    }

    public void markSampleBasedPartitioner() {
        feature = OPER_FEATURE.SAMPLE_BASED_PARTITIONER;
    }

    public void setNeedEstimatedQuantile(boolean needEstimateParallelism) {
        this.needEstimateParallelism = needEstimateParallelism;
    }

    public boolean isNeedEstimateParallelism() {
        return needEstimateParallelism;
    }

    public boolean isUseSecondaryKey() {
        return useSecondaryKey;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public List<OperatorKey> getUnionPredecessors() {
        return vertexGroupMembers;
    }

    public List<OperatorKey> getVertexGroupMembers() {
        return vertexGroupMembers;
    }

    public void addUnionPredecessor(OperatorKey unionPredecessor) {
        if (vertexGroupMembers == null) {
            vertexGroupMembers = new ArrayList<OperatorKey>();
        }
        this.vertexGroupMembers.add(unionPredecessor);
    }

    public void setVertexGroupMembers(List<OperatorKey> vertexGroupMembers) {
        this.vertexGroupMembers = vertexGroupMembers;
    }

    // Union is the only operator that uses alias vertex (VertexGroup) now. But
    // more operators could be added to the list in the future.
    public boolean isVertexGroup() {
        return vertexGroupInfo != null;
    }

    public VertexGroupInfo getVertexGroupInfo() {
        return vertexGroupInfo;
    }

    public void setVertexGroupInfo(VertexGroupInfo vertexGroup) {
        this.vertexGroupInfo = vertexGroup;
    }

    public void addVertexGroupStore(OperatorKey storeKey, OperatorKey vertexGroupKey) {
        if (this.vertexGroupStores == null) {
            this.vertexGroupStores = new HashMap<OperatorKey, OperatorKey>();
        }
        this.vertexGroupStores.put(storeKey, vertexGroupKey);
    }

    public Map<OperatorKey, OperatorKey> getVertexGroupStores() {
        return this.vertexGroupStores;
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

    public void setLimitAfterSort(boolean limitAfterSort) {
        this.limitAfterSort = limitAfterSort;
    }

    public boolean isLimitAfterSort() {
        return limitAfterSort;
    }

    public void setSkewedJoin(boolean skewedJoin) {
        this.skewedJoin = skewedJoin;
    }

    public boolean isSkewedJoin() {
        return skewedJoin;
    }

    protected void noCombineSmallSplits() {
        combineSmallSplits = false;
    }

    public boolean combineSmallSplits() {
        return combineSmallSplits;
    }

    public static class VertexGroupInfo {

        private List<OperatorKey> inputKeys;
        private String outputKey;
        private POStore store;
        private OutputDescriptor storeOutDescriptor;
        private VertexGroup vertexGroup;

        public VertexGroupInfo() {
        }

        public VertexGroupInfo(POStore store) {
            this.store = store;
        }

        public List<OperatorKey> getInputs() {
            return inputKeys;
        }

        public void addInput(OperatorKey input) {
            if (inputKeys == null) {
                inputKeys = new ArrayList<OperatorKey>();
            }
            this.inputKeys.add(input);
        }

        public boolean removeInput(OperatorKey input) {
            return this.inputKeys.remove(input);
        }

        public String getOutput() {
            return outputKey;
        }

        public void setOutput(String output) {
            this.outputKey = output;
        }

        public POStore getStore() {
            return store;
        }

        public OutputDescriptor getStoreOutputDescriptor() {
            return storeOutDescriptor;
        }

        public void setStoreOutputDescriptor(OutputDescriptor storeOutDescriptor) {
            this.storeOutDescriptor = storeOutDescriptor;
        }

        public VertexGroup getVertexGroup() {
            return vertexGroup;
        }

        public void setVertexGroup(VertexGroup vertexGroup) {
            this.vertexGroup = vertexGroup;
        }

    }
}

