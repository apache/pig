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
package org.apache.pig.newplan.logical.visitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.StreamToPig;
import org.apache.pig.impl.builtin.IdentityColumn;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/**
 * Create mapping between uid and Load FuncSpec when the LogicalExpression 
 * associated with it is known to hold an unmodified element of data returned
 * by the load function.
 * This information is used to find the LoadCaster to be used to cast bytearrays
 * into other other types. 
 */
public class LineageFindRelVisitor extends LogicalRelationalNodesVisitor{


    Map<Long, FuncSpec> uid2LoadFuncMap  = new HashMap<Long, FuncSpec>();
    
    //if LogicalRelationalOperator is associated with a single load func spec
    // then the mapping is stored here
    Map<LogicalRelationalOperator, FuncSpec>  rel2InputFuncMap = 
        new HashMap<LogicalRelationalOperator, FuncSpec>();

    Map<FuncSpec, Class> func2casterMap = new HashMap<FuncSpec, Class>();
    
    public LineageFindRelVisitor(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
        
    }
    
    public Map<Long, FuncSpec> getUid2LoadFuncMap(){
        return uid2LoadFuncMap;
    }
    
    @Override
    public void visit(LOLoad load) throws FrontendException{
        LogicalSchema schema = load.getSchema();
        setLoadFuncForUids(schema, load.getFileSpec().getFuncSpec());
        rel2InputFuncMap.put(load, load.getFileSpec().getFuncSpec());
    }

    @Override
    public void visit(LOStream stream) throws FrontendException{
        mapToPredLoadFunc(stream);
        StreamingCommand command = ((LOStream)stream).getStreamingCommand();
        HandleSpec streamOutputSpec = command.getOutputSpec(); 
        FuncSpec streamLoaderSpec = new FuncSpec(streamOutputSpec.getSpec());
        setLoadFuncForUids(stream.getSchema(), streamLoaderSpec);
        rel2InputFuncMap.put(stream, streamLoaderSpec);
    }
    
    @Override
    public void visit(LOInnerLoad innerLoad) throws FrontendException{
        LOForEach foreach = innerLoad.getLOForEach();
        LogicalRelationalOperator pred = 
            ((LogicalRelationalOperator)foreach.getPlan().getPredecessors(foreach).get(0));

        LogicalSchema predSchema = pred.getSchema();
        
        //if this has a schema, the lineage can be tracked using the uid in input schema
        if(innerLoad.getSchema() != null){
            if(innerLoad.getSchema().size() == 1 
                    && innerLoad.getSchema().getField(0).type == DataType.BYTEARRAY
                    && uid2LoadFuncMap.get(innerLoad.getSchema().getField(0).uid) == null
                    && predSchema != null
            ){
                long inpUid = predSchema.getField(innerLoad.getProjection().getColNum()).uid;
                if(uid2LoadFuncMap.get(inpUid) != null){
                    addUidLoadFuncToMap(innerLoad.getSchema().getField(0).uid, uid2LoadFuncMap.get(inpUid));
                }
                return;
            }

        }

        // associated load func could not be found using uid, use
        // the single load func associated with input relation (if any)
        if(getAssociatedLoadFunc(pred) != null){
            mapRelToPredLoadFunc(innerLoad, pred);
        }
    }
    
    /**
     * map all uids in schema to funcSpec 
     * @param schema
     * @param funcSpec
     * @throws VisitorException
     */
    private void setLoadFuncForUids(LogicalSchema schema, FuncSpec funcSpec)
    throws VisitorException {
        if(schema == null){
            return;
        }
        for(LogicalFieldSchema fs : schema.getFields()){
            addUidLoadFuncToMap((Long) fs.uid, funcSpec);
            setLoadFuncForUids(fs.schema, funcSpec);
        }
        
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException{
        mapToPredLoadFunc(filter);
        visitExpression(filter.getFilterPlan());
    }

    /**
     * If all predecessors of relOp are associated with same load 
     * func , then map reOp to it.
     * 
     * @param relOp
     * @throws FrontendException 
     */
    private void mapToPredLoadFunc(LogicalRelationalOperator relOp)
    throws FrontendException {
        OperatorPlan lp = relOp.getPlan();
        List<Operator> preds = lp.getPredecessors(relOp);
        if(lp.getPredecessors(relOp) == null || 
                lp.getPredecessors(relOp).size() == 0){
            // no predecessors , nothing to do
            return;
        }

        FuncSpec loadFuncSpec = getAssociatedLoadFunc((LogicalRelationalOperator) preds.get(0));

        if(loadFuncSpec == null){
            return;
        }
        //ensure that all predecessors are mapped to same load func spec
        for(int i=1; i<preds.size(); i++){
            if(!haveIdenticalCasters(loadFuncSpec,getAssociatedLoadFunc(
                    (LogicalRelationalOperator) preds.get(i)))){
                return;
            }
        }
        rel2InputFuncMap.put(relOp, loadFuncSpec); 
    }

    /**
     * Find single load func spec associated with this relation.
     * If the relation has schema, all uids in schema should be associated
     * with same load func spec. if it does not have schema check the existing
     * mapping
     * @param relOp
     * @return
     * @throws FrontendException
     */
    private FuncSpec getAssociatedLoadFunc(LogicalRelationalOperator relOp) throws FrontendException {
        LogicalSchema schema = relOp.getSchema();
        FuncSpec funcSpec = null;
        if(schema != null){
            if(schema.size() == 0)
                return null;
            funcSpec = uid2LoadFuncMap.get(schema.getField(0).uid);
            if(funcSpec != null) {
                for(int i=1; i<schema.size(); i++){
                    LogicalFieldSchema fs = schema.getField(i);
                    if(! haveIdenticalCasters(funcSpec,
                            uid2LoadFuncMap.get(fs.uid))){
                        //all uid are not associated with same func spec, there is no
                        // single func spec that represents all the fields
                        funcSpec = null;
                        break;
                    }
                }
            }
        }
        
        if(funcSpec == null){
            // If relOp is LOForEach and contains UDF, byte field could come from UDF.
            // We don't assume it share the LoadCaster with predecessor
            if (relOp instanceof LOForEach) {
                UDFFinder udfFinder = new UDFFinder(((LOForEach) relOp).getInnerPlan());
                udfFinder.visit();
                if (udfFinder.getUDFList().size()!=0)
                    return null;
            }
            
            funcSpec = rel2InputFuncMap.get(relOp);
        }

        return funcSpec;
    }

    private void mapRelToPredLoadFunc(LogicalRelationalOperator relOp,
            Operator pred) {
        if(rel2InputFuncMap.get(pred) != null){
            rel2InputFuncMap.put(relOp, rel2InputFuncMap.get(pred));
        }              
    }

    private void visitExpression(LogicalExpressionPlan expPlan)
    throws FrontendException{
        LineageFindExpVisitor findLineageExp =
            new LineageFindExpVisitor(expPlan, uid2LoadFuncMap);
        findLineageExp.visit();        
    }
    
    @Override
    public void visit(LOCogroup group) throws FrontendException{
        mapToPredLoadFunc(group);
        List<Operator> inputs = group.getInputs((LogicalPlan)plan);
        
        // list of field schemas of group plans
        List<LogicalFieldSchema> groupPlanSchemas = new ArrayList<LogicalFieldSchema>();
        
        MultiMap<Integer, LogicalExpressionPlan> plans = group.getExpressionPlans();
        for(LogicalExpressionPlan expPlan : plans.values()){
            visitExpression(expPlan);
            if(expPlan.getSources().size() != 1){
                throw new AssertionError("Group plans should have only one output");
            }
            groupPlanSchemas.add(((LogicalExpression)expPlan.getSources().get(0)).getFieldSchema());
        }
        
        LogicalSchema sch = group.getSchema();

        //if the group plans are associated with same load function , associate
        //same load fucntion with group column schema
        if (getAssociatedLoadFunc(group)!=null) {
            addUidLoadFuncToMap(sch.getField(0).uid, rel2InputFuncMap.get(group));
            if (sch.getField(0).schema!=null)
                setLoadFuncForUids(sch.getField(0).schema, rel2InputFuncMap.get(group));
        }
        else
            mapMatchLoadFuncToUid(sch.getField(0), groupPlanSchemas);
        
        
        
        //set the load func spec for the bags in the schema, this helps if
        // the input schemas are not set
        //group schema has a group column followed by bags corresponding to each
        // input
        if(sch.size() != inputs.size()+1 ){
            throw new AssertionError("cogroup schema size not same as number of inputs");
        }
        
        
        
        for(int i=1; i < sch.size(); i++){
            long uid = sch.getField(i).uid;
            LogicalRelationalOperator input = (LogicalRelationalOperator) inputs.get(i-1);
            if(getAssociatedLoadFunc(input) != null){
                addUidLoadFuncToMap(uid, rel2InputFuncMap.get(input));
            }
        }
        
    }
    

    @Override
    public void visit(LOJoin join) throws FrontendException{
        mapToPredLoadFunc(join);
         MultiMap<Integer, LogicalExpressionPlan> plans = join.getExpressionPlans();
        for(LogicalExpressionPlan expPlan : plans.values()){
            visitExpression(expPlan);
        }
    }
    
    @Override
    public void visit(LOForEach fe) throws FrontendException{
        mapToPredLoadFunc(fe);
        LogicalPlan innerPlan = fe.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
    }
    
    @Override
    public void visit(LOGenerate gen) throws FrontendException{
        mapToPredLoadFunc(gen);
        List<LogicalExpressionPlan> expPlans = gen.getOutputPlans();
        for(LogicalExpressionPlan expPlan : expPlans){
           visitExpression(expPlan);
        }
        
        //associate flatten output to the load func associated with input
        //expression of flatten
        boolean[] flattens = gen.getFlattenFlags();
        for(int i=0; i<flattens.length; i++){
            if(flattens[i] == true){
                //get the output schema corresponding to this exp plan
                gen.getSchema();
                if(gen.getOutputPlanSchemas() == null || gen.getOutputPlanSchemas().size() <= i){
                    return;
                }
                LogicalSchema sch = gen.getOutputPlanSchemas().get(i);
                if(sch == null){
                    continue;
                }
                
                //get the only output exp of the ith plan
                LogicalExpression exp =
                    (LogicalExpression) gen.getOutputPlans().get(i).getSources().get(0);

                //get its funcspec and associate it with uid of all fields in the schema
                FuncSpec funcSpec = uid2LoadFuncMap.get(exp.getFieldSchema().uid);
                for(LogicalFieldSchema fs : sch.getFields()){
                    addUidLoadFuncToMap(fs.uid, funcSpec);
                }
            }
        }
        
    }
    
    @Override
    public void visit(LOSort sort) throws FrontendException{
        mapToPredLoadFunc(sort);
        List<LogicalExpressionPlan> expPlans = sort.getSortColPlans();
        for(LogicalExpressionPlan expPlan : expPlans){
            visitExpression(expPlan);
        }
    }

    @Override
    public void visit(LORank rank) throws FrontendException{
        mapToPredLoadFunc(rank);
        List<LogicalExpressionPlan> expPlans = rank.getRankColPlans();
        for(LogicalExpressionPlan expPlan : expPlans){
            visitExpression(expPlan);
        }
    }

    @Override
    public void visit(LODistinct relOp) throws FrontendException{
        mapToPredLoadFunc(relOp);
    }

    @Override
    public void visit(LOLimit loLimit) throws FrontendException{
        mapToPredLoadFunc(loLimit);
        if (loLimit.getLimitPlan() != null)
            visitExpression(loLimit.getLimitPlan());
    }
    
    @Override
    public void visit(LOSplit relOp) throws FrontendException{
        mapToPredLoadFunc(relOp);
    }
    
    @Override
    public void visit(LOStore relOp) throws FrontendException{
        mapToPredLoadFunc(relOp);
    }


    
    @Override
    public void visit(LOCross relOp) throws FrontendException{
        mapToPredLoadFunc(relOp);
    }

    @Override
    public void visit(LOUnion relOp) throws FrontendException{
        mapToPredLoadFunc(relOp);
        
        // Since the uid changes for Union, add mappings for new uids to funcspec
        LogicalSchema schema = relOp.getSchema();
        if(schema != null){
            // For each output field, checking all the fields being
            // union-ed(bundled) together and only set the funcspec when ALL
            // of them come from the same caster
            //
            // A = (i,j)
            // B = (i,j)
            // C = UNION A, B;
            // Checking if A.i and B.i have the same caster.
            // Same for A.j and B.j
            // A.i and A.j may come from the different casters
            for (LogicalFieldSchema logicalFieldSchema : schema.getFields()) {
                Set<Long> inputs = relOp.getInputUids(logicalFieldSchema.uid);
                if( inputs.size() == 0 ) {
                    // uid was not changed.
                    // funcspec should be already set. skipping
                    continue;
                }
                FuncSpec prevLoadFuncSpec = null, curLoadFuncSpec = null;
                boolean allSameLoader = true;
                for(Long inputUid: inputs) {
                    curLoadFuncSpec = uid2LoadFuncMap.get(inputUid) ;
                    if( prevLoadFuncSpec != null
                      && !haveIdenticalCasters(prevLoadFuncSpec,
                                               curLoadFuncSpec) ) {
                        allSameLoader = false;
                        break;
                    }
                  prevLoadFuncSpec  = curLoadFuncSpec;
                }
                if( allSameLoader ) {
                    addUidLoadFuncToMap(logicalFieldSchema.uid,curLoadFuncSpec);
                }
            }
        }
    }
    
    @Override
    public void visit(LOSplitOutput split) throws FrontendException{
        mapToPredLoadFunc(split);
        visitExpression(split.getFilterPlan());
        
        //The uid changes across the input and output of a split.
        //In this visitor the uids for the fields in output are mapped to ones
        // in input based on position. uid2LoadFuncMap is updated with output 
        // schema uid and load func mapped to corresponding uid in inputschema
        
        
        LogicalSchema inputSch = 
            ((LogicalRelationalOperator)split.getPlan().getPredecessors(split).get(0)).getSchema();

        LogicalSchema outSchema = split.getSchema();
        if(inputSch == null && outSchema == null){
            return;
        }

        if(inputSch == null || outSchema == null){
            String msg = "Bug: in split only one of input/output schema is null "
                + split;
            throw new VisitorException(split, msg,2260, PigException.BUG) ; 
        }
        
        if(inputSch.size() != outSchema.size()){
            String msg = "Bug: input and output schema size of split differ "
                + split;
            throw new VisitorException(split, msg,2261, PigException.BUG) ; 
        }

        for(int i=0; i<inputSch.size(); i++){
            LogicalFieldSchema inField = inputSch.getField(i);
            LogicalFieldSchema outField = outSchema.getField(i);
            if(uid2LoadFuncMap.get(inField.uid) != null){
                addUidLoadFuncToMap(outField.uid, uid2LoadFuncMap.get(inField.uid));
            }
        }
        
    }
    
    
    /**
     * Add given uid, loadFuncSpec to mapping
     * @param uid
     * @param loadFuncSpec
     * @throws VisitorException 
     */
    private void addUidLoadFuncToMap(long uid, FuncSpec loadFuncSpec)
    throws VisitorException{
        if(loadFuncSpec == null){
            return;
        }
        //ensure that uid always matches to same load func
        FuncSpec curFuncSpec = uid2LoadFuncMap.get(uid);
        if(curFuncSpec == null){
            uid2LoadFuncMap.put(uid, loadFuncSpec);
        }else if(! haveIdenticalCasters(curFuncSpec,loadFuncSpec)){
            String msg = "Bug: uid mapped to two different load functions : " +
            curFuncSpec + " and " + loadFuncSpec;
            throw new VisitorException(msg,2262, PigException.BUG) ;
        }
    }
    
    /**
     * if uid in input field schemas or their inner schemas map to same 
     * load function, then map the new uid in bincond also to same
     *  load function in uid2LoadFuncMap
     * @param outFS
     * @param inputFieldSchemas
     * @throws VisitorException 
     */
    void mapMatchLoadFuncToUid(
            LogicalFieldSchema outFS,
            List<LogicalFieldSchema> inputFieldSchemas) throws VisitorException {

        
        if(inputFieldSchemas.size() == 0){
            return;
        }

        // If schema of any input is null, we skip output schema
        for (LogicalFieldSchema fs : inputFieldSchemas) {
            if (fs==null)
                return;
        }
        
        //if same non null load func is associated with all fieldschemas
        // asssociate that with the uid of outFS
        LogicalFieldSchema inpFS1 = inputFieldSchemas.get(0);
        FuncSpec funcSpec1 = uid2LoadFuncMap.get(inpFS1.uid);
        boolean allInnerSchemaMatch = false;
        if(funcSpec1 != null){
            boolean allMatch = true;
            allInnerSchemaMatch = true;
            
            for(LogicalFieldSchema fs : inputFieldSchemas){
                //check if all func spec match
                if(!haveIdenticalCasters(funcSpec1,uid2LoadFuncMap.get(fs.uid))){
                    allMatch = false;
                }
                //check if all inner schema match for use later
                if(outFS.schema == null ||  !outFS.schema.isEqual(fs.schema)){
                    allInnerSchemaMatch = false;
                }
            }
            if(allMatch){
                addUidLoadFuncToMap(outFS.uid, funcSpec1);
            }
        }
        
        //recursively call the function for corresponding files in inner schemas
        if(allInnerSchemaMatch){
            List<LogicalFieldSchema> outFields = outFS.schema.getFields();
            for(int i=0; i<outFields.size(); i++){
                List<LogicalFieldSchema> inFsList = new ArrayList<LogicalFieldSchema>();
                for(LogicalFieldSchema fs : inputFieldSchemas){
                    inFsList.add(fs.schema.getField(i));
                }                        
                mapMatchLoadFuncToUid(outFields.get(i), inFsList);
            }
        }

    }
    


    /**
     * If a input of dereference or map-lookup has associated load function, 
     * the same load function should be associated with the dereference or map-lookup. 
     * 
     */
    class LineageFindExpVisitor extends LogicalExpressionVisitor{

        private Map<Long, FuncSpec> uid2LoadFuncMap;

        public LineageFindExpVisitor(LogicalExpressionPlan plan,
                Map<Long, FuncSpec> uid2LoadFuncMap) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
            this.uid2LoadFuncMap = uid2LoadFuncMap;
        }
        
        @Override
        public void visit(ProjectExpression proj) throws FrontendException {
            // proj should have the same uid as input, if input has schema.
            // if uid does not have associated func spec and input relation 
            // has null schema or it is inner-load, use the load func associated
            // with the relation 
            LogicalRelationalOperator inputRel = proj.findReferent();

            if (proj.getFieldSchema()==null)
                return;
            
            long uid = proj.getFieldSchema().uid;
            if(uid2LoadFuncMap.get(uid) == null && (inputRel.getSchema() == null || inputRel instanceof LOInnerLoad)){
                FuncSpec funcSpec = rel2InputFuncMap.get(inputRel);
                if(funcSpec != null){
                    addUidLoadFuncToMap(uid, funcSpec);
                }
            }
        }
        
        @Override
        public void visit(DereferenceExpression deref) throws FrontendException {
           updateUidMap(deref, deref.getReferredExpression());
        }

        @Override
        public void visit(MapLookupExpression mapLookup) throws FrontendException {
            updateUidMap(mapLookup, mapLookup.getMap());
        }
        
        
        private void updateUidMap(LogicalExpression exp,
                LogicalExpression inp) throws FrontendException {
            //find input uid and corresponding load FuncSpec
            long inpUid = inp.getFieldSchema().uid;
            FuncSpec inpLoadFuncSpec = uid2LoadFuncMap.get(inpUid);
            addUidLoadFuncToMap(exp.getFieldSchema().uid, inpLoadFuncSpec);

        }

        
        @Override
        public void visit(BinCondExpression binCond) throws FrontendException{
            //if either side is a null constant, we can safely associate
            //this bincond with the load func spec on other side
            LogicalExpression lhs = binCond.getLhs();
            LogicalExpression rhs = binCond.getRhs();
            
            if(getNULLConstantInCast(lhs) != null){
                FuncSpec funcSpec = uid2LoadFuncMap.get(rhs.getFieldSchema().uid);
                uid2LoadFuncMap.put(binCond.getFieldSchema().uid, funcSpec);
            }
            else if(getNULLConstantInCast(rhs) != null){
                FuncSpec funcSpec = uid2LoadFuncMap.get(lhs.getFieldSchema().uid);
                uid2LoadFuncMap.put(binCond.getFieldSchema().uid, funcSpec);
            }
            else {
                List<LogicalFieldSchema> inFieldSchemas = new ArrayList<LogicalFieldSchema>();
                inFieldSchemas.add(lhs.getFieldSchema());
                inFieldSchemas.add(rhs.getFieldSchema());
                mapMatchLoadFuncToUid(
                        binCond.getFieldSchema(), 
                        inFieldSchemas
                );
            }
        }
        
        @Override
        public void visit(ScalarExpression scalarExp) throws FrontendException{
            //track lineage to input load function
            
            
            LogicalRelationalOperator inputRel = (LogicalRelationalOperator)scalarExp.getAttachedLogicalOperator();
            LogicalPlan relPlan = (LogicalPlan) inputRel.getPlan();
            List<Operator> softPreds = relPlan.getSoftLinkPredecessors(inputRel);
            
            //1st argument is the column number in input, 2nd arg is the input file name
            Integer inputColNum = (Integer)((ConstantExpression) scalarExp.getArguments().get(0)).getValue();
            String inputFile = (String)((ConstantExpression) scalarExp.getArguments().get(1)).getValue();
            
            long outputUid = scalarExp.getFieldSchema().uid;
            boolean foundInput = false; // a variable to do sanity check on num of input relations

            //find the input relation, and use it to get lineage
            for(Operator softPred : softPreds){
                LOStore inputStore = (LOStore) softPred;
                if(inputStore.getFileSpec().getFileName().equals(inputFile)){
                    
                    if(foundInput == true){
                        throw new FrontendException(
                                "More than one input found for scalar expression",
                                2268,
                                PigException.BUG
                        );
                    }
                    foundInput = true;
                    
                    //found the store corresponding to this scalar expression
                    LogicalSchema sch = inputStore.getSchema();
                    if(sch == null){
                        //see if there is a load function associated with the store
                        FuncSpec funcSpec = rel2InputFuncMap.get(inputStore);
                        addUidLoadFuncToMap(outputUid, funcSpec);
                    }else{
                        //find input uid and corresponding load func
                        LogicalFieldSchema fs = sch.getField(inputColNum);
                        FuncSpec funcSpec = uid2LoadFuncMap.get(fs.uid);
                        addUidLoadFuncToMap(outputUid, funcSpec);
                    }
                }
            }
            
            if(foundInput == false){
                throw new FrontendException(
                        "No input found for scalar expression",
                        2269,
                        PigException.BUG
                );
            }
        }

        /**
         * if there is a null constant under casts, return it
         * @param rel
         * @return
         * @throws FrontendException
         */
        private Object getNULLConstantInCast(LogicalExpression rel) throws FrontendException {
            if(rel instanceof CastExpression){
                if( ((CastExpression)rel).getExpression() instanceof CastExpression)
                    return getNULLConstantInCast(((CastExpression)rel).getExpression());
                else if( ((CastExpression)rel).getExpression() instanceof ConstantExpression){
                    ConstantExpression constExp = (ConstantExpression)((CastExpression)rel).getExpression();
                    if(constExp.getValue() == null)
                        return constExp;
                    else 
                        return null;
                }
            }
            return null;
        }

        
    }

    //Copied from POCast.instantiateFunc
    private Class instantiateCaster(FuncSpec funcSpec) throws VisitorException {
        if ( funcSpec == null ) {
            return null;
        }

        if( func2casterMap.containsKey(funcSpec) ) {
          return func2casterMap.get(funcSpec);
        }

        LoadCaster caster = null;
        Object obj = PigContext.instantiateFuncFromSpec(funcSpec);
        try {
            if (obj instanceof LoadFunc) {
                caster = ((LoadFunc)obj).getLoadCaster();
            } else if (obj instanceof StreamToPig) {
                caster = ((StreamToPig)obj).getLoadCaster();
            } else {
                throw new VisitorException("Invalid class type " + funcSpec.getClassName(),
                                           2270, PigException.BUG );
            }
        } catch (IOException e) {
            throw new VisitorException("Invalid class type " + funcSpec.getClassName(),
                                         2270, e );
        }

        Class retval = (caster == null) ? null : caster.getClass();
        func2casterMap.put(funcSpec, retval);
        return retval;
    }


    private boolean haveIdenticalCasters(FuncSpec f1, FuncSpec f2)  throws VisitorException{
        if( f1 == null || f2 == null ) {
            return false;
        }
        if( f1.equals(f2) ) {
            return true;
        }
        Class caster1 = instantiateCaster(f1);
        Class caster2 = instantiateCaster(f2);

        if( caster1 == null || caster2 == null ) {
            return false;
        }

        //From PIG-3295
        //If the class name for LoadCaster are the same, and LoadCaster only has
        //default constructor, then two LoadCasters are considered equal
        if( caster1.getCanonicalName().equals(caster2.getCanonicalName())  &&
            caster1.getConstructors().length == 1 &&
            caster1.getConstructors()[0].getGenericParameterTypes().length == 0  &&
            caster2.getConstructors().length == 1 &&
            caster2.getConstructors()[0].getGenericParameterTypes().length == 0 ) {
            return true;
        }

        return false;

    }
    
    
}
