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
package org.apache.pig.backend.hadoop.executionengine.tez.plan;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigProcessor;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;

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

    // Do not estimate parallelism for specific vertices like limit, indexer,
    // etc which should always be one
    private boolean dontEstimateParallelism = false;

    // This is the parallelism of the vertex, it take account of:
    // 1. default_parallel
    // 2. -1 parallelism for one_to_one edge
    // 3. -1 parallelism for sort/skewed join
    private int vertexParallelism = -1;

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

    //The sort order of the columns;
    //asc is true and desc is false
    boolean[] sortOrder;

    // Flag to indicate if the small input splits need to be combined to form a larger
    // one in order to reduce the number of mappers. For merge join, both tables
    // are NOT combinable for correctness.
    private boolean combineSmallSplits = true;

    // Used by partition vertex, if not null, need to collect sample sent from predecessor
    private TezOperator sampleOperator = null;

    // Used by sample vertex, send parallelism event to orderOperator
    private TezOperator sortOperator = null;

    // If the flag is set, FindQuantilesTez/PartitionSkewedKeysTez will use aggregated sample
    // to calculate the number of parallelism at runtime, instead of the numQuantiles/totalReducers_
    // parameter set statically
    private boolean needEstimateParallelism = false;

    // If true, we will use secondary key sort in the job
    private boolean useSecondaryKey = false;

    private String crossKey = null;

    private boolean useMRMapSettings = false;

    // Types of blocking operators. For now, we only support the following ones.
    public static enum OPER_FEATURE {
        // Indicate if this job is a merge indexer
        INDEXER,
        // Indicate if this job is a sampling job
        SAMPLER,
        // Indicate if this job is a sample aggregation job
        SAMPLE_AGGREGATOR,
        // Indicate if this job is a sample based partition job (order by/skewed join)
        SAMPLE_BASED_PARTITIONER,
        // Indicate if this job is a global sort
        GLOBAL_SORT,
        // Indicate if this job is a group by job
        GROUPBY,
        // Indicate if this job is a cogroup job
        COGROUP,
        // Indicate if this job is a regular join job
        HASHJOIN,
        // Indicate if this job is a skewed join job
        SKEWEDJOIN,
        // Indicate if this job is a limit job
        LIMIT,
        // Indicate if this job is a limit job after sort
        LIMIT_AFTER_SORT,
        // Indicate if this job is a union job
        UNION,
        // Indicate if this job is a native job
        NATIVE;
    };

    // Features in the job/vertex. Mostly will be only one feature.
    // But in some cases can have more than one.
    // For eg: a vertex can be both GLOBAL SORT and LIMIT if parallelism is 1
    BitSet feature = new BitSet();

    private List<OperatorKey> vertexGroupMembers;
    // For union
    private VertexGroupInfo vertexGroupInfo;
    // Mapping of OperatorKey of POStore OperatorKey to vertexGroup TezOperator
    private Map<OperatorKey, OperatorKey> vertexGroupStores = null;

    public static class LoaderInfo {
        private List<POLoad> loads = null;
        private ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        private ArrayList<String> inpSignatureLists = new ArrayList<String>();
        private ArrayList<Long> inpLimits = new ArrayList<Long>();
        private InputSplitInfo inputSplitInfo = null;
        public List<POLoad> getLoads() {
            return loads;
        }
        public void setLoads(List<POLoad> loads) {
            this.loads = loads;
        }
        public ArrayList<FileSpec> getInp() {
            return inp;
        }
        public void setInp(ArrayList<FileSpec> inp) {
            this.inp = inp;
        }
        public ArrayList<String> getInpSignatureLists() {
            return inpSignatureLists;
        }
        public void setInpSignatureLists(ArrayList<String> inpSignatureLists) {
            this.inpSignatureLists = inpSignatureLists;
        }
        public ArrayList<Long> getInpLimits() {
            return inpLimits;
        }
        public void setInpLimits(ArrayList<Long> inpLimits) {
            this.inpLimits = inpLimits;
        }
        public InputSplitInfo getInputSplitInfo() {
            return inputSplitInfo;
        }
        public void setInputSplitInfo(InputSplitInfo inputSplitInfo) {
            this.inputSplitInfo = inputSplitInfo;
        }
    }

    private LoaderInfo loaderInfo = new LoaderInfo();

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
        // PIG-4162: For intermediate reducers, use estimated parallelism over user set parallelism.
        return getEstimatedParallelism() == -1 ? getRequestedParallelism()
                : getEstimatedParallelism();
    }

    public boolean isDontEstimateParallelism() {
        return dontEstimateParallelism;
    }

    public void setDontEstimateParallelism(boolean dontEstimateParallelism) {
        this.dontEstimateParallelism = dontEstimateParallelism;
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

    public boolean isIndexer() {
        return feature.get(OPER_FEATURE.INDEXER.ordinal());
    }

    public void markIndexer() {
        feature.set(OPER_FEATURE.INDEXER.ordinal());
    }

    public boolean isSampler() {
        return feature.get(OPER_FEATURE.SAMPLER.ordinal());
    }

    public void markSampler() {
        feature.set(OPER_FEATURE.SAMPLER.ordinal());
    }

    public boolean isSampleAggregation() {
        return feature.get(OPER_FEATURE.SAMPLE_AGGREGATOR.ordinal());
    }

    public void markSampleAggregation() {
        feature.set(OPER_FEATURE.SAMPLE_AGGREGATOR.ordinal());
    }

    public boolean isSampleBasedPartitioner() {
        return feature.get(OPER_FEATURE.SAMPLE_BASED_PARTITIONER.ordinal());
    }

    public void markSampleBasedPartitioner() {
        feature.set(OPER_FEATURE.SAMPLE_BASED_PARTITIONER.ordinal());
    }

    public boolean isGlobalSort() {
        return feature.get(OPER_FEATURE.GLOBAL_SORT.ordinal());
    }

    public void markGlobalSort() {
        feature.set(OPER_FEATURE.GLOBAL_SORT.ordinal());
    }

    public boolean isGroupBy() {
        return feature.get(OPER_FEATURE.GROUPBY.ordinal());
    }

    public void markGroupBy() {
        feature.set(OPER_FEATURE.GROUPBY.ordinal());
    }

    public boolean isCogroup() {
        return feature.get(OPER_FEATURE.COGROUP.ordinal());
    }

    public void markCogroup() {
        feature.set(OPER_FEATURE.COGROUP.ordinal());
    }

    public boolean isRegularJoin() {
        return feature.get(OPER_FEATURE.HASHJOIN.ordinal());
    }

    public void markRegularJoin() {
        feature.set(OPER_FEATURE.HASHJOIN.ordinal());
    }

    public boolean isSkewedJoin() {
        return feature.get(OPER_FEATURE.SKEWEDJOIN.ordinal());
    }

    public void markSkewedJoin() {
        feature.set(OPER_FEATURE.SKEWEDJOIN.ordinal());
    }

    public boolean isLimit() {
        return feature.get(OPER_FEATURE.LIMIT.ordinal());
    }

    public void markLimit() {
        feature.set(OPER_FEATURE.LIMIT.ordinal());
    }

    public boolean isLimitAfterSort() {
        return feature.get(OPER_FEATURE.LIMIT_AFTER_SORT.ordinal());
    }

    public void markLimitAfterSort() {
        feature.set(OPER_FEATURE.LIMIT_AFTER_SORT.ordinal());
    }

    public boolean isUnion() {
        return feature.get(OPER_FEATURE.UNION.ordinal());
    }

    public void markUnion() {
        feature.set(OPER_FEATURE.UNION.ordinal());
    }

    public boolean isNative() {
        return feature.get(OPER_FEATURE.NATIVE.ordinal());
    }

    public void markNative() {
        feature.set(OPER_FEATURE.NATIVE.ordinal());
    }

    public void copyFeatures(TezOperator copyFrom, List<OPER_FEATURE> excludeFeatures) {
        for (OPER_FEATURE opf : OPER_FEATURE.values()) {
            if (excludeFeatures != null && excludeFeatures.contains(opf)) {
                continue;
            }
            if (copyFrom.feature.get(opf.ordinal())) {
                feature.set(opf.ordinal());
            }
        }
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

    public TezOperator getSampleOperator() {
        return sampleOperator;
    }

    public void setSampleOperator(TezOperator sampleOperator) {
        this.sampleOperator = sampleOperator;
    }

    public TezOperator getSortOperator() {
        return sortOperator;
    }

    public void setSortOperator(TezOperator sortOperator) {
        this.sortOperator = sortOperator;
    }

    protected void noCombineSmallSplits() {
        combineSmallSplits = false;
    }

    public boolean combineSmallSplits() {
        return combineSmallSplits;
    }

    public void setCrossKey(String key) {
        crossKey = key;
    }

    public String getCrossKey() {
        return crossKey;
    }

    public boolean isUseMRMapSettings() {
        return useMRMapSettings;
    }

    public void setUseMRMapSettings(boolean useMRMapSettings) {
        this.useMRMapSettings = useMRMapSettings;
    }

    public int getVertexParallelism() {
        return vertexParallelism;
    }

    public void setVertexParallelism(int vertexParallelism) {
        this.vertexParallelism = vertexParallelism;
    }

    public LoaderInfo getLoaderInfo() {
        return loaderInfo;
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

