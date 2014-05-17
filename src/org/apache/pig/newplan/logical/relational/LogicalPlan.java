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

package org.apache.pig.newplan.logical.relational;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigConstants;
import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.HashOutputStream;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.DotLOPrinter;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.visitor.CastLineageSetter;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;
import org.apache.pig.newplan.logical.visitor.DanglingNestedNodeRemover;
import org.apache.pig.newplan.logical.visitor.DuplicateForEachColumnRewriteVisitor;
import org.apache.pig.newplan.logical.visitor.ImplicitSplitInsertVisitor;
import org.apache.pig.newplan.logical.visitor.InputOutputFileValidatorVisitor;
import org.apache.pig.newplan.logical.visitor.ScalarVariableValidator;
import org.apache.pig.newplan.logical.visitor.ScalarVisitor;
import org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor;
import org.apache.pig.newplan.logical.visitor.SortInfoSetter;
import org.apache.pig.newplan.logical.visitor.StoreAliasSetter;
import org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor;
import org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter;
import org.apache.pig.pen.POOptimizeDisabler;
import org.apache.pig.validator.BlackAndWhitelistValidator;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * LogicalPlan is the logical view of relational operations Pig will execute
 * for a given script.  Note that it contains only relational operations.
 * All expressions will be contained in LogicalExpressionPlans inside
 * each relational operator.
 */
public class LogicalPlan extends BaseOperatorPlan {

    public LogicalPlan(LogicalPlan other) {
        // shallow copy constructor
        super(other);
    }

    public LogicalPlan() {
        super();
    }

    /**
     * Equality is checked by calling equals on every leaf in the plan.  This
     * assumes that plans are always connected graphs.  It is somewhat
     * inefficient since every leaf will test equality all the way to
     * every root.  But it is only intended for use in testing, so that
     * should be ok.  Checking predecessors (as opposed to successors) was
     * chosen because splits (which have multiple successors) do not depend
     * on order of outputs for correctness, whereas joins (with multiple
     * predecessors) do.  That is, reversing the outputs of split in the
     * graph has no correctness implications, whereas reversing the inputs
     * of join can.  This method of doing equals will detect predecessors
     * in different orders but not successors in different orders.
     * It will return false if either plan has non deterministic EvalFunc.
     */
    @Override
    public boolean isEqual(OperatorPlan other) throws FrontendException {
        if (other == null || !(other instanceof LogicalPlan)) {
            return false;
        }

        return super.isEqual(other);
    }

    @Override
    public void explain(PrintStream ps, String format, boolean verbose)
    throws FrontendException {
        if (format.equals("xml")) {
            ps.println("<logicalPlan>XML Not Supported</logicalPlan>");
            return;
        }

        ps.println("#-----------------------------------------------");
        ps.println("# New Logical Plan:");
        ps.println("#-----------------------------------------------");

        if (this.size() == 0) {
            ps.println("Logical plan is empty.");
        } else if (format.equals("dot")) {
            DotLOPrinter lpp = new DotLOPrinter(this, ps);
            lpp.dump();
        } else {
            LogicalPlanPrinter npp = new LogicalPlanPrinter(this, ps);
            npp.visit();
        }
    }

    public Operator findByAlias(String alias) {
    	Iterator<Operator> it = getOperators();
    	List<Operator> ops = new ArrayList<Operator>();
    	while( it.hasNext() ) {
    	    LogicalRelationalOperator op = (LogicalRelationalOperator) it.next();
    	    if(op.getAlias() == null)
    	        continue;
    	    if(op.getAlias().equals( alias ) )  {
    	        ops.add( op );
    	    }
    	}

    	if( ops.isEmpty() ) {
            return null;
    	} else {
    		return ops.get( ops.size() - 1 ); // Last one
    	}
    }

    /**
     * Returns the signature of the LogicalPlan. The signature is a unique identifier for a given
     * plan generated by a Pig script. The same script run multiple times with the same version of
     * Pig is guaranteed to produce the same signature, even if the input or output locations differ.
     *
     * @return a unique identifier for the logical plan
     * @throws FrontendException if signature can't be computed
     */
    public String getSignature() throws FrontendException {

        // Use a streaming hash function. We use a murmur_32 function with a constant seed, 0.
        HashFunction hf = Hashing.murmur3_32(0);
        HashOutputStream hos = new HashOutputStream(hf);
        PrintStream ps = new PrintStream(hos);

        LogicalPlanPrinter printer = new LogicalPlanPrinter(this, ps);
        printer.visit();

        return Integer.toString(hos.getHashCode().asInt());
    }
    
    public void validate(PigContext pigContext, String scope) throws FrontendException {

        new DanglingNestedNodeRemover(this).visit();
        new ColumnAliasConversionVisitor(this).visit();
        new SchemaAliasVisitor(this).visit();
        new ScalarVisitor(this, pigContext, scope).visit();

        // ImplicitSplitInsertVisitor has to be called before
        // DuplicateForEachColumnRewriteVisitor.  Detail at pig-1766
        new ImplicitSplitInsertVisitor(this).visit();

        // DuplicateForEachColumnRewriteVisitor should be before
        // TypeCheckingRelVisitor which does resetSchema/getSchema
        // heavily
        new DuplicateForEachColumnRewriteVisitor(this).visit();

        CompilationMessageCollector collector = new CompilationMessageCollector() ;

        new TypeCheckingRelVisitor( this, collector).visit();

        boolean aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));

        if(aggregateWarning) {
            CompilationMessageCollector.logMessages(collector, MessageType.Warning, aggregateWarning, log);
        } else {
            for(Enum type: MessageType.values()) {
                CompilationMessageCollector.logAllMessages(collector, log);
            }
        }

        new UnionOnSchemaSetter(this).visit();
        new CastLineageSetter(this, collector).visit();
        new ScalarVariableValidator(this).visit();
        new StoreAliasSetter(this).visit();

        // compute whether output data is sorted or not
        new SortInfoSetter(this).visit();

        if (!(pigContext.inExplain || pigContext.inDumpSchema)) {
            // Validate input/output file
            new InputOutputFileValidatorVisitor(this, pigContext).visit();
        }

        BlackAndWhitelistValidator validator = new BlackAndWhitelistValidator(pigContext, this);
        validator.validate();

        // Now make sure the plan is consistent
        UidResetter uidResetter = new UidResetter(this);
        uidResetter.visit();

        SchemaResetter schemaResetter = new SchemaResetter(this,
                true /* skip duplicate uid check*/);
        schemaResetter.visit();
    }
    
    public void optimize(PigContext pigContext) throws FrontendException {
        if (pigContext.inIllustrator) {
            // disable all PO-specific optimizations
            POOptimizeDisabler pod = new POOptimizeDisabler(this);
            pod.visit();
        }

        HashSet<String> disabledOptimizerRules;
        try {
            disabledOptimizerRules = (HashSet<String>) ObjectSerializer
                    .deserialize(pigContext.getProperties().getProperty(
                            PigImplConstants.PIG_OPTIMIZER_RULES_KEY));
        } catch (IOException ioe) {
            int errCode = 2110;
            String msg = "Unable to deserialize optimizer rules.";
            throw new FrontendException(msg, errCode, PigException.BUG, ioe);
        }
        if (disabledOptimizerRules == null) {
            disabledOptimizerRules = new HashSet<String>();
        }

        String pigOptimizerRulesDisabled = pigContext.getProperties()
                .getProperty(PigConstants.PIG_OPTIMIZER_RULES_DISABLED_KEY);
        if (pigOptimizerRulesDisabled != null) {
            disabledOptimizerRules.addAll(Lists.newArrayList((Splitter.on(",")
                    .split(pigOptimizerRulesDisabled))));
        }

        if (pigContext.inIllustrator) {
            disabledOptimizerRules.add("MergeForEach");
            disabledOptimizerRules.add("PartitionFilterOptimizer");
            disabledOptimizerRules.add("LimitOptimizer");
            disabledOptimizerRules.add("SplitFilter");
            disabledOptimizerRules.add("PushUpFilter");
            disabledOptimizerRules.add("MergeFilter");
            disabledOptimizerRules.add("PushDownForEachFlatten");
            disabledOptimizerRules.add("ColumnMapKeyPrune");
            disabledOptimizerRules.add("AddForEach");
            disabledOptimizerRules.add("GroupByConstParallelSetter");
        }

        // run optimizer
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(this, 100,
                disabledOptimizerRules);
        optimizer.optimize();
    }
}
