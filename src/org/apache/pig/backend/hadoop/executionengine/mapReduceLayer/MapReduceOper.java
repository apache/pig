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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * An operator model for a Map Reduce job. 
 * Acts as a host to the plans that will
 * execute in map, reduce and optionally combine
 * phases. These will be embedded in the MROperPlan
 * in order to capture the dependencies amongst jobs.
 */
public class MapReduceOper extends Operator<MROpPlanVisitor> {
    private static final long serialVersionUID = 1L;

    //The physical plan that should be executed
    //in the map phase
    public PhysicalPlan mapPlan;
    
    //The physical plan that should be executed
    //in the reduce phase
    public PhysicalPlan reducePlan;
    
    //The physical plan that should be executed
    //in the combine phase if one exists. Will be used
    //by the optimizer.
    public PhysicalPlan combinePlan;
    
    // key for the map plan
    // this is needed when the key is null to create
    // an appropriate NullableXXXWritable object
    public byte mapKeyType;
    
    //Indicates that the map plan creation
    //is complete
    boolean mapDone = false;
    
    //Indicates that the reduce plan creation
    //is complete
    boolean reduceDone = false;
    
    // Indicates that there is an operator which uses endOfAllInput flag in the 
    // map plan
    boolean endOfAllInputInMap = false;
    
    // Indicates that there is an operator which uses endOfAllInput flag in the 
    // reduce plan
    boolean endOfAllInputInReduce = false;;
    
    //Indicates if this job is an order by job
    boolean globalSort = false;

    // Indicates if this is a limit after a sort
    boolean limitAfterSort = false;
    
    // Indicate if the entire purpose for this map reduce job is doing limit, does not change
    // anything else. This is to help POPackageAnnotator to find the right POPackage to annotate
    boolean limitOnly = false;
    
    OPER_FEATURE feature = OPER_FEATURE.NONE;

    // If true, putting an identity combine in this
    // mapreduce job will speed things up.
    boolean needsDistinctCombiner = false;
    
    // If true, we will use secondary key in the map-reduce job
    boolean useSecondaryKey = false;
    
    //The quantiles file name if globalSort is true
    String quantFile;
    
    //The sort order of the columns;
    //asc is true and desc is false
    boolean[] sortOrder;
    
    // Sort order for secondary keys;
    boolean[] secondarySortOrder;

    public Set<String> UDFs;
    
    public Set<PhysicalOperator> scalars;
    
    // Indicates if a UDF comparator is used
    boolean isUDFComparatorUsed = false;
    
    transient NodeIdGenerator nig;

    private String scope;
    
    int requestedParallelism = -1;
    
    // estimated at runtime
    int estimatedParallelism = -1;
    
    // calculated at runtime 
    int runtimeParallelism = -1;
    
    /* Name of the Custom Partitioner used */ 
    String customPartitioner = null;
    
    // Last POLimit value in this map reduce operator, needed by LimitAdjuster
    // to add additional map reduce operator with 1 reducer after this
    long limit = -1;

    // POLimit can also have an expression. See PIG-1926
    PhysicalPlan limitPlan = null;
    
    // Indicates that this MROper is a splitter MROper. 
    // That is, this MROper ends due to a POSPlit operator.
    private boolean splitter = false;

	// Set to true if it is skewed join
	private boolean skewedJoin = false;

    // Name of the partition file generated by sampling process,
    // Used by Skewed Join
	private String skewedJoinPartitionFile;
	
	// Flag to communicate from MRCompiler to JobControlCompiler what kind of
	// comparator is used by Hadoop for sorting for this MROper. 
	// By default, set to false which will make Pig provide raw comparators. 
	// Set to true in indexing job generated in map-side cogroup, merge join.
	private boolean usingTypedComparator = false;
	
	// Flag to indicate if the small input splits need to be combined to form a larger
	// one in order to reduce the number of mappers. For merge join, both tables
	// are NOT combinable for correctness.
	private boolean combineSmallSplits = true;
	
	// Map of the physical operator in physical plan to the one in MR plan: only needed
	// if the physical operator is changed/replaced in MR compilation due to, e.g., optimization
	public MultiMap<PhysicalOperator, PhysicalOperator> phyToMRMap;
	
	private static enum OPER_FEATURE {
	    NONE,
	    // Indicate if this job is a sampling job
	    SAMPLER,
	    // Indicate if this job is a merge indexer
	    INDEXER,
	    // Indicate if this job is a group by job
	    GROUPBY,	    
	    // Indicate if this job is a cogroup job
	    COGROUP,	    
	    // Indicate if this job is a regular join job
	    HASHJOIN;
	};
	
    public MapReduceOper(OperatorKey k) {
        super(k);
        mapPlan = new PhysicalPlan();
        combinePlan = new PhysicalPlan();
        reducePlan = new PhysicalPlan();
        UDFs = new HashSet<String>();
        scalars = new HashSet<PhysicalOperator>();
        nig = NodeIdGenerator.getGenerator();
        scope = k.getScope();
        phyToMRMap = new MultiMap<PhysicalOperator, PhysicalOperator>();
    }

    /*@Override
    public String name() {
        return "MapReduce - " + mKey.toString();
    }*/
    
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
    
    /**
     * Uses the string representation of the 
     * component plans to identify itself.
     */
    @Override
    public String name() {
        String udfStr = getUDFsAsStr();
        
        StringBuilder sb = new StringBuilder("MapReduce" + "(" + requestedParallelism + 
                (udfStr.equals("")? "" : ",") + udfStr + ")" + " - " + mKey.toString()
                + ":\n");
        int index = sb.length();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if(!mapPlan.isEmpty()){
            mapPlan.explain(baos);
            String mp = new String(baos.toByteArray());
            sb.append(shiftStringByTabs(mp, "|   "));
        }
        else
            sb.append("Map Plan Empty");
        if (!reducePlan.isEmpty()){
            baos.reset();
            reducePlan.explain(baos);
            String rp = new String(baos.toByteArray());
            sb.insert(index, shiftStringByTabs(rp, "|   ") + "\n");
        }
        else
            sb.insert(index, "Reduce Plan Empty" + "\n");
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

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public void visit(MROpPlanVisitor v) throws VisitorException {
        v.visitMROp(this);
    }
    
    public boolean isMapDone() {
        return mapDone;
    }
    
    public void setMapDone(boolean mapDone){
        this.mapDone = mapDone;
    }
    
    public void setMapDoneSingle(boolean mapDone) throws PlanException{
        this.mapDone = mapDone;
        if (mapDone && mapPlan.getLeaves().size()>1) {
            mapPlan.addAsLeaf(getUnion());
        }
    }
    
    public void setMapDoneMultiple(boolean mapDone) throws PlanException{
        this.mapDone = mapDone;
        if (mapDone && mapPlan.getLeaves().size()>0) {
            mapPlan.addAsLeaf(getUnion());
        }
    }
    
    private POUnion getUnion(){
        return new POUnion(new OperatorKey(scope,nig.getNextNodeId(scope)));
    }
    
    public boolean isReduceDone() {
        return reduceDone;
    }

    public void setReduceDone(boolean reduceDone){
        this.reduceDone = reduceDone;
    }
    
    public boolean isGlobalSort() {
        return globalSort;
    }
    
    public boolean isSkewedJoin() {
    	return (skewedJoinPartitionFile != null);
    }
    
    public void setSkewedJoinPartitionFile(String file) {    	
    	skewedJoinPartitionFile = file;
    }
    
    public String getSkewedJoinPartitionFile() {
    	return skewedJoinPartitionFile;
    }

	public void setSkewedJoin(boolean skJoin) {
		this.skewedJoin = skJoin;
	}

	public boolean getSkewedJoin() {
		return skewedJoin;
	}

    public void setGlobalSort(boolean globalSort) {
        this.globalSort = globalSort;
    }

    public boolean isLimitAfterSort() {
        return limitAfterSort;
    }

    public void setLimitAfterSort(boolean las) {
        limitAfterSort = las;
    }
    
    public boolean isLimitOnly() {
        return limitOnly;
    }
    
    public void setLimitOnly(boolean limitOnly) {
        this.limitOnly = limitOnly;
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
    
    public boolean needsDistinctCombiner() { 
        return needsDistinctCombiner;
    }

    public void setNeedsDistinctCombiner(boolean nic) {
        needsDistinctCombiner = nic;
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
    
    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        if(null == secondarySortOrder) return;
        this.secondarySortOrder = new boolean[secondarySortOrder.length];
        for(int i = 0; i < secondarySortOrder.length; ++i) {
            this.secondarySortOrder[i] = secondarySortOrder[i];
        }
    }
             
    public boolean[] getSortOrder() {
        return sortOrder;
    }

    public boolean[] getSecondarySortOrder() {
        return secondarySortOrder;
    }

    /**
     * @return whether end of all input is set in the map plan
     */
    public boolean isEndOfAllInputSetInMap() {
        return endOfAllInputInMap;
    }

    /**
     * @param endOfAllInputInMap the streamInMap to set
     */
    public void setEndOfAllInputInMap(boolean endOfAllInputInMap) {
        this.endOfAllInputInMap = endOfAllInputInMap;
    }

    /**
     * @return whether end of all input is set in the reduce plan
     */
    public boolean isEndOfAllInputSetInReduce() {
        return endOfAllInputInReduce;
    }

    /**
     * @param endOfAllInputInReduce the streamInReduce to set
     */
    public void setEndOfAllInputInReduce(boolean endOfAllInputInReduce) {
        this.endOfAllInputInReduce = endOfAllInputInReduce;
    }    

    public int getRequestedParallelism() {
        return requestedParallelism;
    }
    
    public String getCustomPartitioner() {
    	return customPartitioner;
    }

    public void setSplitter(boolean spl) {
        splitter = spl;
    }

    public boolean isSplitter() {
        return splitter;
    }
    
    public boolean getUseSecondaryKey() {
        return useSecondaryKey;
    }
    
    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    protected boolean usingTypedComparator() {
        return usingTypedComparator;
    }

    protected void useTypedComparator(boolean useTypedComparator) {
        this.usingTypedComparator = useTypedComparator;
    }
    
    protected void noCombineSmallSplits() {
        combineSmallSplits = false;
    }
    
    public boolean combineSmallSplits() {
        return combineSmallSplits;
    }
}
