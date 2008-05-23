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
package org.apache.pig.impl.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * An operator model for a Map Reduce job. 
 * Acts as a host to the plans that will
 * execute in map, reduce and optionally combine
 * phases. These will be embedded in the MROperPlan
 * in order to capture the dependecies amongst jobs.
 */
public class MapReduceOper extends Operator<MROpPlanVisitor> {
    //The physical plan that should be executed
    //in the map phase
    public PhysicalPlan<PhysicalOperator> mapPlan;
    
    //The physical plan that should be executed
    //in the reduce phase
    public PhysicalPlan<PhysicalOperator> reducePlan;
    
    //The physical plan that should be executed
    //in the combine phase if one exists. Will be used
    //by the optimizer.
    public PhysicalPlan<PhysicalOperator> combinePlan;
    
    //Indicates that the map plan creation
    //is complete
    boolean mapDone = false;
    
    //Indicates that the reduce plan creation
    //is complete
    boolean reduceDone = false;
    
    //Indicates if this job is an order by job
    boolean globalSort = false;
    
    //The quantiles file name if globalSort is true
    String quantFile;
    
    public List<String> UDFs;
    
    NodeIdGenerator nig;

    private String scope;
    
    int requestedParallelism = -1;

    public MapReduceOper(OperatorKey k) {
        super(k);
        mapPlan = new PhysicalPlan<PhysicalOperator>();
        combinePlan = new PhysicalPlan<PhysicalOperator>();
        reducePlan = new PhysicalPlan<PhysicalOperator>();
        UDFs = new ArrayList<String>();
        nig = NodeIdGenerator.getGenerator();
        scope = "MapReduceOper";
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
    
    /**
     * Utility method for the MRCompiler that
     * creates a union operator and adds it as
     * a leaf to the map plan. Used when there
     * is more than one input in order to process
     * them in a single map job  
     * @param mapDone
     * @throws IOException
     *//*
    public void setMapDoneAndMerge(boolean mapDone) throws IOException{
        this.mapDone = mapDone;
        if(mapDone){
            mapPlan.addAsLeaf(GenPhyOp.topUnionOp());
        }
    }
    
    *//**
     * Same as the above method but checks to see
     * there is more than one input. It is for 
     * optimization purposes as a single input
     * pipeline really doesn't need the union operator
     * to execute in a single job.
     * @param mapDone
     * @throws IOException
     *//*
    public void setMapDoneAndChkdMerge(boolean mapDone) throws IOException {
        this.mapDone = mapDone;
        if (mapDone && mapPlan.getLeaves().size()!=1) {
            mapPlan.addAsLeaf(GenPhyOp.topUnionOp());
        }
    }*/

    public boolean isReduceDone() {
        return reduceDone;
    }

    public void setReduceDone(boolean reduceDone){
        this.reduceDone = reduceDone;
    }
    
    public boolean isGlobalSort() {
        return globalSort;
    }

    public void setGlobalSort(boolean globalSort) {
        this.globalSort = globalSort;
    }

    public String getQuantFile() {
        return quantFile;
    }

    public void setQuantFile(String quantFile) {
        this.quantFile = quantFile;
    }
}
