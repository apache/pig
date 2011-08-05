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

package org.apache.pig.pen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Collection;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackageLite;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCombinerPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POIsNull;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartitionRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POOptimizedForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

/**
 * The class used to (re)attach illustrators to physical operators
 * 
 *
 */
public class IllustratorAttacher extends PhyPlanVisitor {

    PigContext pigContext;

    LineageTracer lineage;

    HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>> poToEqclassesMap;
    
    private HashMap<PhysicalOperator, DataBag> poToDataMap;
    private int maxRecords;
    private boolean revisit = false;
    private ArrayList<Boolean[]> subExpResults = null;
    private final Map<POLoad, LogicalSchema> poloadToSchemaMap;
    
    public IllustratorAttacher(PhysicalPlan plan, LineageTracer lineage, int maxRecords,
        Map<POLoad, LogicalSchema> poLoadToSchemaMap, PigContext hadoopPigContext) throws VisitorException {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        pigContext = hadoopPigContext;
        this.lineage = lineage;
        poToEqclassesMap = new HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>>();
        poToDataMap = new HashMap<PhysicalOperator, DataBag>();
        this.maxRecords = maxRecords;
        this.poloadToSchemaMap = poLoadToSchemaMap;
    }

    /**
     * revisit an enhanced physical plan from MR compilation
     * @param plan a physical plan to be traversed
     */
    public void revisit(PhysicalPlan plan) throws VisitorException {
        pushWalker(new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        revisit = true;
        PhysicalPlan oriPlan = mPlan;
        mPlan = plan;
        visit();
        mPlan = oriPlan;
        popWalker();
    }
    
    private void setIllustrator(PhysicalOperator po, int nEqClasses) {
        if (revisit && po.getIllustrator() != null)
            return;
        LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
        poToEqclassesMap.put(po, eqClasses);
        for (int i = 0; i < nEqClasses; ++i)
        {
            IdentityHashSet<Tuple> eqClass = new IdentityHashSet<Tuple>();
            eqClasses.add(eqClass);
        }
        Illustrator illustrator = new Illustrator(lineage, eqClasses, this, pigContext);
        po.setIllustrator(illustrator);
        poToDataMap.put(po, illustrator.getData());
    }
    
    private void setIllustrator(PhysicalOperator po, LinkedList<IdentityHashSet<Tuple>> eqClasses) {
        if (revisit && po.getIllustrator() != null)
            return;
        Illustrator illustrator = new Illustrator(lineage, eqClasses, this, pigContext);
        po.setIllustrator(illustrator);
        if (eqClasses != null)
            poToEqclassesMap.put(po, eqClasses);
        poToDataMap.put(po, illustrator.getData());
    }

    void setIllustrator(PhysicalOperator po) {
        if (revisit && po.getIllustrator() != null)
            return;
        LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
        IdentityHashSet<Tuple> eqClass = new IdentityHashSet<Tuple>();
        eqClasses.add(eqClass);
        Illustrator illustrator = new Illustrator(lineage, eqClasses, this, pigContext);
        po.setIllustrator(illustrator);
        poToEqclassesMap.put(po, eqClasses);
        poToDataMap.put(po, illustrator.getData());
    }
    
    public Map<PhysicalOperator, DataBag> getDataMap() {
        return poToDataMap;
    }
    
    @Override
    public void visitLoad(POLoad ld) throws VisitorException{
        // LOAD from temporary files need no illustrator 
        if (revisit)
            return;
        
        LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
        poToEqclassesMap.put(ld, eqClasses);
        
        IdentityHashSet<Tuple> eqClass = new IdentityHashSet<Tuple>();
        eqClasses.add(eqClass);
        Illustrator illustrator;
        illustrator = new Illustrator(lineage, eqClasses, maxRecords, this, poloadToSchemaMap.get(ld), pigContext);
        ld.setIllustrator(illustrator);
        poToDataMap.put(ld, illustrator.getData());
    }
    
    @Override
    public void visitStore(POStore st) throws VisitorException{
        setIllustrator(st, 1);
    }
    
    @Override
    public void visitFilter(POFilter fl) throws VisitorException{
        setIllustrator(fl, 0);
        subExpResults = fl.getIllustrator().getSubExpResults();
        innerPlanAttach(fl, fl.getPlan());
        subExpResults = null;
    }
    
    @Override
    public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException{
      super.visitLocalRearrange(lr);
      setIllustrator(lr);
    }
    
    @Override
    public void visitPackage(POPackage pkg) throws VisitorException{
        if (!(pkg instanceof POPackageLite) && pkg.isDistinct())
            setIllustrator(pkg, 1);
        else
            setIllustrator(pkg, null);
    }
    
    @Override
    public void visitCombinerPackage(POCombinerPackage pkg) throws VisitorException{
        setIllustrator(pkg);
    }
 
    @Override
    public void visitMultiQueryPackage(POMultiQueryPackage pkg) throws VisitorException{
      setIllustrator(pkg);
    }
    
    @Override
    public void visitPOForEach(POForEach nfe) throws VisitorException {
        if (revisit && nfe.getIllustrator() != null)
            return;
        List<PhysicalPlan> innerPlans = nfe.getInputPlans();
        for (PhysicalPlan innerPlan : innerPlans)
          innerPlanAttach(nfe, innerPlan);
        List<PhysicalOperator> preds = mPlan.getPredecessors(nfe);
        if (preds != null && preds.size() == 1 &&
            preds.get(0) instanceof POPackage &&
            !(preds.get(0) instanceof POPackageLite) &&
            ((POPackage) preds.get(0)).isDistinct()) {
            // equivalence class of POPackage for DISTINCT needs to be used
            //instead of the succeeding POForEach's equivalence class
            setIllustrator(nfe, preds.get(0).getIllustrator().getEquivalenceClasses());
            nfe.getIllustrator().setEqClassesShared();
        } else
            setIllustrator(nfe, 1);
    }
    
    @Override
    public void visitUnion(POUnion un) throws VisitorException{
        if (revisit && un.getIllustrator() != null)
          return;
        setIllustrator(un, null);
    }
    
    @Override
    public void visitSplit(POSplit spl) throws VisitorException{
        if (revisit && spl.getIllustrator() != null)
            return;
        for (PhysicalPlan poPlan : spl.getPlans())
          innerPlanAttach(spl, poPlan);
        setIllustrator(spl);
    }

    @Override
    public void visitDemux(PODemux demux) throws VisitorException{
      if (revisit && demux.getIllustrator() != null)
          return;
      List<PhysicalPlan> innerPlans = demux.getPlans();
      for (PhysicalPlan innerPlan : innerPlans)
        innerPlanAttach(demux, innerPlan);
      setIllustrator(demux);
    }
    
    @Override
	public void visitDistinct(PODistinct distinct) throws VisitorException {
        setIllustrator(distinct, 1);
	}
    
    @Override
	public void visitSort(POSort sort) throws VisitorException {
        setIllustrator(sort, 1);
	}
    
    @Override
    public void visitProject(POProject proj) throws VisitorException{
    }
    
    @Override
    public void visitGreaterThan(GreaterThanExpr grt) throws VisitorException{
        setIllustrator(grt, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(grt.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitLessThan(LessThanExpr lt) throws VisitorException{
        setIllustrator(lt, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(lt.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitGTOrEqual(GTOrEqualToExpr gte) throws VisitorException{
        setIllustrator(gte, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(gte.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitLTOrEqual(LTOrEqualToExpr lte) throws VisitorException{
        setIllustrator(lte, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(lte.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitEqualTo(EqualToExpr eq) throws VisitorException{
        setIllustrator(eq, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(eq.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitNotEqualTo(NotEqualToExpr eq) throws VisitorException{
        setIllustrator(eq, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(eq.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitRegexp(PORegexp re) throws VisitorException{
        setIllustrator(re, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(re.getIllustrator().getSubExpResult());
    }

    @Override
    public void visitIsNull(POIsNull isNull) throws VisitorException {
        setIllustrator(isNull, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(isNull.getIllustrator().getSubExpResult());
    }
    
    @Override
    public void visitAnd(POAnd and) throws VisitorException {
        setIllustrator(and, 0);
    }
    
    @Override
    public void visitOr(POOr or) throws VisitorException {
        setIllustrator(or, 0);
    }

    @Override
    public void visitNot(PONot not) throws VisitorException {
        setIllustrator(not, 0);
        if (!revisit && subExpResults != null)
            subExpResults.add(not.getIllustrator().getSubExpResult());
    }

    @Override
    public void visitBinCond(POBinCond binCond) {

    }

    @Override
    public void visitNegative(PONegative negative) {
      setIllustrator(negative, 1);
    }
    
    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
    }
    
    @Override
    public void visitComparisonFunc(POUserComparisonFunc compFunc) throws VisitorException {
        // one each for >, ==, and <
        setIllustrator(compFunc, 3);
    }

    @Override
    public void visitMapLookUp(POMapLookUp mapLookUp) {
      setIllustrator(mapLookUp, 1);
    }
    
    @Override
    public void visitJoinPackage(POJoinPackage joinPackage) throws VisitorException{
        if (revisit &&  joinPackage.getIllustrator() != null)
            return;
        setIllustrator(joinPackage);
        joinPackage.getForEach().setIllustrator(joinPackage.getIllustrator());
    }

    @Override
    public void visitCast(POCast cast) {
    }
    
    @Override
    public void visitLimit(POLimit lim) throws VisitorException {
        setIllustrator(lim, 1);
    }
    
    @Override
    public void visitStream(POStream stream) throws VisitorException {
        setIllustrator(stream, 1);
    }

    /**
     * @param optimizedForEach
     */
    @Override
    public void visitPOOptimizedForEach(POOptimizedForEach optimizedForEach) throws VisitorException {
        visitPOForEach(optimizedForEach);
    }
    
    private void innerPlanAttach(PhysicalOperator po, PhysicalPlan plan) throws VisitorException {
        PlanWalker<PhysicalOperator, PhysicalPlan> childWalker =
              mCurrentWalker.spawnChildWalker(plan);
        pushWalker(childWalker);
        childWalker.walk(this);
        popWalker();
        LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
        if (subExpResults != null && !revisit) {
            int size = 1 << subExpResults.size();
            for (int i = 0; i < size; ++i) {
                eqClasses.add(new IdentityHashSet<Tuple>());
            }
            po.getIllustrator().setEquivalenceClasses(eqClasses, po);
        }
    }
}
