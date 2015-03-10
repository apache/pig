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
package org.apache.pig.backend.hadoop.executionengine.spark.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.CollectableLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.ScalarPhyFinder;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POStreamSpark;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The compiler that compiles a given physical physicalPlan into a DAG of Spark
 * operators
 */
public class SparkCompiler extends PhyPlanVisitor {
	private PigContext pigContext;

	// The physicalPlan that is being compiled
	private PhysicalPlan physicalPlan;

	// The physicalPlan of Spark Operators
	private SparkOperPlan sparkPlan;

	private SparkOperator curSparkOp;

	private String scope;

	private SparkOperator[] compiledInputs = null;

	private Map<OperatorKey, SparkOperator> splitsSeen;

	private NodeIdGenerator nig;

	private Map<PhysicalOperator, SparkOperator> phyToSparkOpMap;
	private UDFFinder udfFinder;

	public SparkCompiler(PhysicalPlan physicalPlan, PigContext pigContext) {
		super(physicalPlan,
				new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
						physicalPlan));
		this.physicalPlan = physicalPlan;
		this.pigContext = pigContext;
		this.sparkPlan = new SparkOperPlan();
		this.phyToSparkOpMap = new HashMap<PhysicalOperator, SparkOperator>();
		this.udfFinder = new UDFFinder();
		this.nig = NodeIdGenerator.getGenerator();
		this.splitsSeen = new HashMap<OperatorKey, SparkOperator>();

	}

	public void compile() throws IOException, PlanException, VisitorException {
		List<PhysicalOperator> roots = physicalPlan.getRoots();
		if ((roots == null) || (roots.size() <= 0)) {
			int errCode = 2053;
			String msg = "Internal error. Did not find roots in the physical physicalPlan.";
			throw new SparkCompilerException(msg, errCode, PigException.BUG);
		}
		scope = roots.get(0).getOperatorKey().getScope();
		List<PhysicalOperator> leaves = physicalPlan.getLeaves();

		if (!pigContext.inIllustrator)
			for (PhysicalOperator op : leaves) {
				if (!(op instanceof POStore)) {
					int errCode = 2025;
					String msg = "Expected leaf of reduce physicalPlan to "
							+ "always be POStore. Found "
							+ op.getClass().getSimpleName();
					throw new SparkCompilerException(msg, errCode,
							PigException.BUG);
				}
			}

		// get all stores and nativeSpark operators, sort them in order(operator
		// id)
		// and compile their plans
		List<POStore> stores = PlanHelper.getPhysicalOperators(physicalPlan,
				POStore.class);
		List<PONative> nativeSparks = PlanHelper.getPhysicalOperators(
				physicalPlan, PONative.class);
		List<PhysicalOperator> ops;
		if (!pigContext.inIllustrator) {
			ops = new ArrayList<PhysicalOperator>(stores.size()
					+ nativeSparks.size());
			ops.addAll(stores);
		} else {
			ops = new ArrayList<PhysicalOperator>(leaves.size()
					+ nativeSparks.size());
			ops.addAll(leaves);
		}
		ops.addAll(nativeSparks);
		Collections.sort(ops);

		for (PhysicalOperator op : ops) {
			compile(op);
		}
	}

	/**
	 * Compiles the physicalPlan below op into a Spark Operator and stores it in
	 * curSparkOp.
	 * 
	 * @param op
	 * @throws IOException
	 * @throws PlanException
	 * @throws VisitorException
	 */
	private void compile(PhysicalOperator op) throws IOException,
			PlanException, VisitorException {
		SparkOperator[] prevCompInp = compiledInputs;

		List<PhysicalOperator> predecessors = physicalPlan.getPredecessors(op);
		if (op instanceof PONative) {
			// the predecessor (store) has already been processed
			// don't process it again
		} else if (predecessors != null && predecessors.size() > 0) {
			// When processing an entire script (multiquery), we can
			// get into a situation where a load has
			// predecessors. This means that it depends on some store
			// earlier in the physicalPlan. We need to take that dependency
			// and connect the respective Spark operators, while at the
			// same time removing the connection between the Physical
			// operators. That way the jobs will run in the right
			// order.
			if (op instanceof POLoad) {

				if (predecessors.size() != 1) {
					int errCode = 2125;
					String msg = "Expected at most one predecessor of load. Got "
							+ predecessors.size();
					throw new PlanException(msg, errCode, PigException.BUG);
				}

				PhysicalOperator p = predecessors.get(0);
				SparkOperator oper = null;
				if (p instanceof POStore || p instanceof PONative) {
					oper = phyToSparkOpMap.get(p);
				} else {
					int errCode = 2126;
					String msg = "Predecessor of load should be a store or spark operator. Got "
							+ p.getClass();
					throw new PlanException(msg, errCode, PigException.BUG);
				}

				// Need new operator
				curSparkOp = getSparkOp();
				curSparkOp.add(op);
				sparkPlan.add(curSparkOp);
				physicalPlan.disconnect(op, p);
				sparkPlan.connect(oper, curSparkOp);
				phyToSparkOpMap.put(op, curSparkOp);
				return;
			}

			Collections.sort(predecessors);
			compiledInputs = new SparkOperator[predecessors.size()];
			int i = -1;
			for (PhysicalOperator pred : predecessors) {
				if (pred instanceof POSplit
						&& splitsSeen.containsKey(pred.getOperatorKey())) {
					compiledInputs[++i] = startNew(
							((POSplit) pred).getSplitStore(),
							splitsSeen.get(pred.getOperatorKey()));
					continue;
				}
				compile(pred);
				compiledInputs[++i] = curSparkOp;
			}
		} else {
			// No predecessors. Mostly a load. But this is where
			// we start. We create a new sparkOp and add its first
			// operator op. Also this should be added to the sparkPlan.
			curSparkOp = getSparkOp();
			curSparkOp.add(op);
			if (op != null && op instanceof POLoad) {
				if (((POLoad) op).getLFile() != null
						&& ((POLoad) op).getLFile().getFuncSpec() != null)
					curSparkOp.UDFs.add(((POLoad) op).getLFile().getFuncSpec()
							.toString());
			}
			sparkPlan.add(curSparkOp);
			phyToSparkOpMap.put(op, curSparkOp);
			return;
		}
		op.visit(this);
		compiledInputs = prevCompInp;
	}

	private SparkOperator getSparkOp() {
		return new SparkOperator(OperatorKey.genOpKey(scope));
	}

	public SparkOperPlan getSparkPlan() {
		return sparkPlan;
	}

	public void connectSoftLink() throws PlanException, IOException {
		for (PhysicalOperator op : physicalPlan) {
			if (physicalPlan.getSoftLinkPredecessors(op) != null) {
				for (PhysicalOperator pred : physicalPlan
						.getSoftLinkPredecessors(op)) {
					SparkOperator from = phyToSparkOpMap.get(pred);
					SparkOperator to = phyToSparkOpMap.get(op);
					if (from == to)
						continue;
					if (sparkPlan.getPredecessors(to) == null
							|| !sparkPlan.getPredecessors(to).contains(from)) {
						sparkPlan.connect(from, to);
					}
				}
			}
		}
	}

	private SparkOperator startNew(FileSpec fSpec, SparkOperator old)
			throws PlanException {
		POLoad ld = getLoad();
		ld.setLFile(fSpec);
		SparkOperator ret = getSparkOp();
		ret.add(ld);
		sparkPlan.add(ret);
		sparkPlan.connect(old, ret);
		return ret;
	}

	private POLoad getLoad() {
		POLoad ld = new POLoad(new OperatorKey(scope, nig.getNextNodeId(scope)));
		ld.setPc(pigContext);
		ld.setIsTmpLoad(true);
		return ld;
	}

	@Override
	public void visitSplit(POSplit op) throws VisitorException {
		try {
			FileSpec fSpec = op.getSplitStore();
			SparkOperator sparkOp = endSingleInputPlanWithStr(fSpec);
			sparkOp.setSplitter(true);
			splitsSeen.put(op.getOperatorKey(), sparkOp);
			curSparkOp = startNew(fSpec, sparkOp);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	public void visitDistinct(PODistinct op) throws VisitorException {
		try {
			nonBlocking(op);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	private SparkOperator endSingleInputPlanWithStr(FileSpec fSpec)
			throws PlanException {
		if (compiledInputs.length > 1) {
			int errCode = 2023;
			String msg = "Received a multi input physicalPlan when expecting only a single input one.";
			throw new PlanException(msg, errCode, PigException.BUG);
		}
		SparkOperator sparkOp = compiledInputs[0]; // Load
		POStore str = getStore();
		str.setSFile(fSpec);
		sparkOp.physicalPlan.addAsLeaf(str);
		return sparkOp;
	}

	private POStore getStore() {
		POStore st = new POStore(new OperatorKey(scope,
				nig.getNextNodeId(scope)));
		// mark store as tmp store. These could be removed by the
		// optimizer, because it wasn't the user requesting it.
		st.setIsTmpStore(true);
		return st;
	}

	@Override
	public void visitLoad(POLoad op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitNative(PONative op) throws VisitorException {
		try {
			SparkOperator nativesparkOpper = getNativeSparkOp(
					op.getNativeMRjar(), op.getParams());
			sparkPlan.add(nativesparkOpper);
			sparkPlan.connect(curSparkOp, nativesparkOpper);
			phyToSparkOpMap.put(op, nativesparkOpper);
			curSparkOp = nativesparkOpper;
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	private NativeSparkOperator getNativeSparkOp(String sparkJar,
			String[] parameters) {
		return new NativeSparkOperator(new OperatorKey(scope,
				nig.getNextNodeId(scope)), sparkJar, parameters);
	}

	@Override
	public void visitStore(POStore op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
			if (op.getSFile() != null && op.getSFile().getFuncSpec() != null)
				curSparkOp.UDFs.add(op.getSFile().getFuncSpec().toString());
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitFilter(POFilter op) throws VisitorException {
		try {
			nonBlocking(op);
			processUDFs(op.getPlan());
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitCross(POCross op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitStream(POStream op) throws VisitorException {
		try {
			POStreamSpark poStreamSpark = new POStreamSpark(op);
			nonBlocking(poStreamSpark);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitSort(POSort op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitLimit(POLimit op) throws VisitorException {
		try {
			nonBlocking(op);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitLocalRearrange(POLocalRearrange op)
			throws VisitorException {
		try {
			nonBlocking(op);
			List<PhysicalPlan> plans = op.getPlans();
			if (plans != null)
				for (PhysicalPlan ep : plans)
					processUDFs(ep);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitCollectedGroup(POCollectedGroup op)
			throws VisitorException {
		List<PhysicalOperator> roots = curSparkOp.physicalPlan.getRoots();
		if (roots.size() != 1) {
			int errCode = 2171;
			String errMsg = "Expected one but found more then one root physical operator in physical physicalPlan.";
			throw new SparkCompilerException(errMsg, errCode, PigException.BUG);
		}

		PhysicalOperator phyOp = roots.get(0);
		if (!(phyOp instanceof POLoad)) {
			int errCode = 2172;
			String errMsg = "Expected physical operator at root to be POLoad. Found : "
					+ phyOp.getClass().getCanonicalName();
			throw new SparkCompilerException(errMsg, errCode, PigException.BUG);
		}

		LoadFunc loadFunc = ((POLoad) phyOp).getLoadFunc();
		try {
			if (!(CollectableLoadFunc.class.isAssignableFrom(loadFunc
					.getClass()))) {
				int errCode = 2249;
				throw new SparkCompilerException(
						"While using 'collected' on group; data must be loaded via loader implementing CollectableLoadFunc.",
						errCode);
			}
			((CollectableLoadFunc) loadFunc).ensureAllKeyInstancesInSameSplit();
		} catch (SparkCompilerException e) {
			throw (e);
		} catch (IOException e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}

		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitPOForEach(POForEach op) throws VisitorException {
		try {
			nonBlocking(op);
			List<PhysicalPlan> plans = op.getInputPlans();
			if (plans != null) {
				for (PhysicalPlan ep : plans) {
					processUDFs(ep);
				}
			}
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitGlobalRearrange(POGlobalRearrange op)
			throws VisitorException {
		try {
			blocking(op);
			curSparkOp.customPartitioner = op.getCustomPartitioner();
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitPackage(POPackage op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
			if (op.getPkgr().getPackageType() == Packager.PackageType.JOIN) {
				curSparkOp.markRegularJoin();
			} else if (op.getPkgr().getPackageType() == Packager.PackageType.GROUP) {
				if (op.getNumInps() == 1) {
					curSparkOp.markGroupBy();
				} else if (op.getNumInps() > 1) {
					curSparkOp.markCogroup();
				}
			}

		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitUnion(POUnion op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitSkewedJoin(POSkewedJoin op) throws VisitorException {
		// TODO
	}

	@Override
	public void visitFRJoin(POFRJoin op) throws VisitorException {
		// TODO
	}

	@Override
	public void visitMergeJoin(POMergeJoin joinOp) throws VisitorException {
		// TODO
	}

	private void processUDFs(PhysicalPlan plan) throws VisitorException {
		if (plan != null) {
			// Process Scalars (UDF with referencedOperators)
			ScalarPhyFinder scalarPhyFinder = new ScalarPhyFinder(plan);
			scalarPhyFinder.visit();
			curSparkOp.scalars.addAll(scalarPhyFinder.getScalars());

			// Process UDFs
			udfFinder.setPlan(plan);
			udfFinder.visit();
			curSparkOp.UDFs.addAll(udfFinder.getUDFs());
		}
	}

	private void nonBlocking(PhysicalOperator op) throws PlanException,
			IOException {
		SparkOperator sparkOp = null;
		if (compiledInputs.length == 1) {
			sparkOp = compiledInputs[0];
		} else {
			sparkOp = merge(compiledInputs);
		}
		sparkOp.physicalPlan.addAsLeaf(op);
		curSparkOp = sparkOp;
	}

	private void blocking(PhysicalOperator op) throws PlanException,
			IOException {
		SparkOperator sparkOp = getSparkOp();
		sparkPlan.add(sparkOp);
		for (SparkOperator compileInput : compiledInputs) {
			sparkPlan.connect(compileInput, sparkOp);
		}
		sparkOp.physicalPlan.addAsLeaf(op);
		curSparkOp = sparkOp;
	}

	private SparkOperator merge(SparkOperator[] compiledInputs)
			throws PlanException {
		SparkOperator ret = getSparkOp();
		sparkPlan.add(ret);

		Set<SparkOperator> toBeConnected = new HashSet<SparkOperator>();
		List<SparkOperator> toBeRemoved = new ArrayList<SparkOperator>();

		List<PhysicalPlan> toBeMerged = new ArrayList<PhysicalPlan>();

		for (SparkOperator sparkOp : compiledInputs) {
			toBeRemoved.add(sparkOp);
			toBeMerged.add(sparkOp.physicalPlan);
			List<SparkOperator> predecessors = sparkPlan
					.getPredecessors(sparkOp);
			if (predecessors != null) {
				for (SparkOperator predecessorSparkOp : predecessors) {
					toBeConnected.add(predecessorSparkOp);
				}
			}
		}
		merge(ret.physicalPlan, toBeMerged);

		Iterator<SparkOperator> it = toBeConnected.iterator();
		while (it.hasNext())
			sparkPlan.connect(it.next(), ret);
		for (SparkOperator removeSparkOp : toBeRemoved) {
			if (removeSparkOp.requestedParallelism > ret.requestedParallelism)
				ret.requestedParallelism = removeSparkOp.requestedParallelism;
			for (String udf : removeSparkOp.UDFs) {
				if (!ret.UDFs.contains(udf))
					ret.UDFs.add(udf);
			}
			// We also need to change scalar marking
			for (PhysicalOperator physOp : removeSparkOp.scalars) {
				if (!ret.scalars.contains(physOp)) {
					ret.scalars.add(physOp);
				}
			}
			Set<PhysicalOperator> opsToChange = new HashSet<PhysicalOperator>();
			for (Map.Entry<PhysicalOperator, SparkOperator> entry : phyToSparkOpMap
					.entrySet()) {
				if (entry.getValue() == removeSparkOp) {
					opsToChange.add(entry.getKey());
				}
			}
			for (PhysicalOperator op : opsToChange) {
				phyToSparkOpMap.put(op, ret);
			}

			sparkPlan.remove(removeSparkOp);
		}
		return ret;
	}

	/**
	 * The merge of a list of plans into a single physicalPlan
	 * 
	 * @param <O>
	 * @param <E>
	 * @param finPlan
	 *            - Final Plan into which the list of plans is merged
	 * @param plans
	 *            - list of plans to be merged
	 * @throws PlanException
	 */
	private <O extends Operator<?>, E extends OperatorPlan<O>> void merge(
			E finPlan, List<E> plans) throws PlanException {
		for (E e : plans) {
			finPlan.merge(e);
		}
	}
}
