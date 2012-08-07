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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.util;

import java.net.URI;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Divide;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Mod;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Multiply;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POIsNull;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCombinerPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POOptimizedForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartitionRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

import com.google.common.collect.Lists;

/**
 * Utility class with a few helper functions to deal with physical plans.
 */
public class PlanHelper {

    private final static Log log = LogFactory.getLog(new PlanHelper().getClass());

    private PlanHelper() {}

    /**
     * Creates a relative path that can be used to build a temporary
     * place to store the output from a number of map-reduce tasks.
     */
    public static String makeStoreTmpPath(String orig) {
        Path path = new Path(orig);
        URI uri = path.toUri();
        uri.normalize();

        String pathStr = uri.getPath();
        if (path.isAbsolute()) {
            return new Path("abs"+pathStr).toString();
        } else {
            return new Path("rel/"+pathStr).toString();
        }
    }

    public static <C extends PhysicalOperator> boolean containsPhysicalOperator(PhysicalPlan plan,
            Class<C> opClass) throws VisitorException {
        OpFinder<C> finder = new OpFinder<C>(plan, opClass);
        finder.visit();
        return finder.planContainsOp();
    }

    /**
     * Returns a LinkedList of operators contained within the physical plan which implement the supplied class, in dependency order.
     * Returns an empty LinkedList of no such operators exist.
     * @param plan
     * @param opClass
     * @return a LinkedList of operators contained within the plan which implement the supplied class; empty if no such ops exist.
     * @throws VisitorException
     */
    public static <C extends PhysicalOperator> LinkedList<C> getPhysicalOperators(PhysicalPlan plan,
            Class<C> opClass) throws VisitorException {
        OpFinder<C> finder = new OpFinder<C>(plan, opClass);
        finder.visit();
        return finder.getFoundOps();
    }

    private static class OpFinder<C extends PhysicalOperator> extends PhyPlanVisitor {

        final Class<C> opClass;
        private LinkedList<C> foundOps = Lists.newLinkedList();

        public OpFinder(PhysicalPlan plan,
                Class<C> opClass) {
            super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.opClass = opClass;
        }

        public LinkedList<C> getFoundOps() {
            return foundOps;
        }

        public boolean planContainsOp() {
            return !foundOps.isEmpty();
        }

        @SuppressWarnings("unchecked")
        private void visit(PhysicalOperator op) {
            if (opClass.isAssignableFrom(op.getClass())) {
                foundOps.add((C) op);
            }
        }

        @Override
        public void visitLoad(POLoad ld) throws VisitorException {
            super.visitLoad(ld);
            visit(ld);
        }

        @Override
        public void visitStore(POStore st) throws VisitorException {
            super.visitStore(st);
            visit(st);
        }

        @Override
        public void visitNative(PONative nat) throws VisitorException {
            super.visitNative(nat);
            visit(nat);
        }

        @Override
        public void visitFilter(POFilter fl) throws VisitorException {
            super.visitFilter(fl);
            visit(fl);
        }

        @Override
        public void visitCollectedGroup(POCollectedGroup mg)
                throws VisitorException {
            super.visitCollectedGroup(mg);
            visit(mg);
        }

        @Override
        public void visitLocalRearrange(POLocalRearrange lr)
                throws VisitorException {
            super.visitLocalRearrange(lr);
            visit(lr);
        }

        @Override
        public void visitGlobalRearrange(POGlobalRearrange gr)
                throws VisitorException {
            super.visitGlobalRearrange(gr);
            visit(gr);
        }

        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            super.visitPackage(pkg);
            visit(pkg);
        }

        @Override
        public void visitCombinerPackage(POCombinerPackage pkg)
                throws VisitorException {
            super.visitCombinerPackage(pkg);
            visit(pkg);
        }

        @Override
        public void visitMultiQueryPackage(POMultiQueryPackage pkg)
                throws VisitorException {
            super.visitMultiQueryPackage(pkg);
            visit(pkg);
        }

        @Override
        public void visitPOForEach(POForEach nfe) throws VisitorException {
            super.visitPOForEach(nfe);
            visit(nfe);
        }

        @Override
        public void visitUnion(POUnion un) throws VisitorException {
            super.visitUnion(un);
            visit(un);
        }

        @Override
        public void visitSplit(POSplit spl) throws VisitorException {
            super.visitSplit(spl);
            visit(spl);
        }

        @Override
        public void visitDemux(PODemux demux) throws VisitorException {
            super.visitDemux(demux);
            visit(demux);
        }

        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            super.visitDistinct(distinct);
            visit(distinct);
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            super.visitSort(sort);
            visit(sort);
        }

        @Override
        public void visitConstant(ConstantExpression cnst)
                throws VisitorException {
            super.visitConstant(cnst);
            visit(cnst);
        }

        @Override
        public void visitProject(POProject proj) throws VisitorException {
            super.visitProject(proj);
            visit(proj);
        }

        @Override
        public void visitGreaterThan(GreaterThanExpr grt)
                throws VisitorException {
            super.visitGreaterThan(grt);
            visit(grt);
        }

        @Override
        public void visitLessThan(LessThanExpr lt) throws VisitorException {
            super.visitLessThan(lt);
            visit(lt);
        }

        @Override
        public void visitGTOrEqual(GTOrEqualToExpr gte) throws VisitorException {
            super.visitGTOrEqual(gte);
            visit(gte);
        }

        @Override
        public void visitLTOrEqual(LTOrEqualToExpr lte) throws VisitorException {
            super.visitLTOrEqual(lte);
            visit(lte);
        }

        @Override
        public void visitEqualTo(EqualToExpr eq) throws VisitorException {
            super.visitEqualTo(eq);
            visit(eq);
        }

        @Override
        public void visitNotEqualTo(NotEqualToExpr eq) throws VisitorException {
            super.visitNotEqualTo(eq);
            visit(eq);
        }

        @Override
        public void visitRegexp(PORegexp re) throws VisitorException {
            super.visitRegexp(re);
            visit(re);
        }

        @Override
        public void visitIsNull(POIsNull isNull) throws VisitorException {
            super.visitIsNull(isNull);
            visit(isNull);
        }

        @Override
        public void visitAdd(Add add) throws VisitorException {
            super.visitAdd(add);
            visit(add);
        }

        @Override
        public void visitSubtract(Subtract sub) throws VisitorException {
            super.visitSubtract(sub);
            visit(sub);
        }

        @Override
        public void visitMultiply(Multiply mul) throws VisitorException {
            super.visitMultiply(mul);
            visit(mul);
        }

        @Override
        public void visitDivide(Divide dv) throws VisitorException {
            super.visitDivide(dv);
            visit(dv);
        }

        @Override
        public void visitMod(Mod mod) throws VisitorException {
            super.visitMod(mod);
            visit(mod);
        }

        @Override
        public void visitAnd(POAnd and) throws VisitorException {
            super.visitAnd(and);
            visit(and);
        }

        @Override
        public void visitOr(POOr or) throws VisitorException {
            super.visitOr(or);
            visit(or);
        }

        @Override
        public void visitNot(PONot not) throws VisitorException {
            super.visitNot(not);
            visit(not);
        }

        @Override
        public void visitBinCond(POBinCond binCond) {
            super.visitBinCond(binCond);
            visit(binCond);
        }

        @Override
        public void visitNegative(PONegative negative) {
            super.visitNegative(negative);
            visit(negative);
        }

        @Override
        public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
            super.visitUserFunc(userFunc);
            visit(userFunc);
        }

        @Override
        public void visitComparisonFunc(POUserComparisonFunc compFunc)
                throws VisitorException {
            super.visitComparisonFunc(compFunc);
            visit(compFunc);
    }

        @Override
        public void visitMapLookUp(POMapLookUp mapLookUp) {
            super.visitMapLookUp(mapLookUp);
            visit(mapLookUp);
        }

        @Override
        public void visitJoinPackage(POJoinPackage joinPackage)
                throws VisitorException {
            super.visitJoinPackage(joinPackage);
            visit(joinPackage);
        }

        @Override
        public void visitCast(POCast cast) {
            super.visitCast(cast);
            visit(cast);
        }

        @Override
        public void visitLimit(POLimit lim) throws VisitorException {
            super.visitLimit(lim);
            visit(lim);
        }

        @Override
        public void visitCross(POCross cross) throws VisitorException {
            super.visitCross(cross);
            visit(cross);
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            super.visitFRJoin(join);
            visit(join);
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            super.visitMergeJoin(join);
            visit(join);
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
            super.visitMergeCoGroup(mergeCoGrp);
            visit(mergeCoGrp);
        }

        @Override
        public void visitStream(POStream stream) throws VisitorException {
            super.visitStream(stream);
            visit(stream);
        }

        @Override
        public void visitSkewedJoin(POSkewedJoin sk) throws VisitorException {
            super.visitSkewedJoin(sk);
            visit(sk);
        }

        @Override
        public void visitPartitionRearrange(POPartitionRearrange pr)
                throws VisitorException {
            super.visitPartitionRearrange(pr);
            visit(pr);
        }

        @Override
        public void visitPOOptimizedForEach(POOptimizedForEach optimizedForEach)
                throws VisitorException {
            super.visitPOOptimizedForEach(optimizedForEach);
            visit(optimizedForEach);
        }

        @Override
        public void visitPreCombinerLocalRearrange(
                POPreCombinerLocalRearrange preCombinerLocalRearrange) {
            super.visitPreCombinerLocalRearrange(preCombinerLocalRearrange);
            visit(preCombinerLocalRearrange);
        }

        @Override
        public void visitPartialAgg(POPartialAgg poPartialAgg) {
            super.visitPartialAgg(poPartialAgg);
            visit(poPartialAgg);
        }
    }

}
