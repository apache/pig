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
package org.apache.pig.penny;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.pig.FuncSpec;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.penny.impl.harnesses.CoordinatorHarness;
import org.apache.pig.penny.impl.pig.MonitorAgentUDF;
import org.apache.pig.penny.impl.pig.MonitorAgentUDFArgs;
import org.apache.pig.penny.impl.pig.PigLauncher;
import org.apache.pig.tools.ToolsPigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;


public class ParsedPigScript {
    
    private final ToolsPigServer pigServer;
    private final String pigScript;                                            // user's raw pig script
    private final LogicalPlan queryPlan;                                    // pig's logical query plan object
    private Map<String, LogicalRelationalOperator> aliasOperatorMap;        // the pig LogicalRelationalOperator corresponding to each alias

  // Ibis change : start
  // the pig LOStore corresponding to each alias which is actually a 'LOStore'
  private Map<String, LOStore> storeMap;
  // Ibis change : end

    private final Random rand = new Random(System.currentTimeMillis());

  public ParsedPigScript(PigContext pigContext, String pigScriptFilename) throws IOException {
        this.pigServer = new ToolsPigServer(pigContext);
        this.pigScript = pigScriptFilename;
        pigServer.registerNoRun(pigScript, new HashMap<String, String>(), new LinkedList<String>());
        this.queryPlan = pigServer.getPlans().lp;
        /* debug: */ //System.out.println("ORIGINAL PLAN: " + queryPlan);
        aliasOperatorMap = new HashMap<String, LogicalRelationalOperator>();
    // Ibis change : start
    this.storeMap = new HashMap<String, LOStore>();
    // Ibis change : end
        populateAliasOperatorMap(queryPlan, aliasOperatorMap, storeMap);
    }

  // Ibis change : start
    private static void populateAliasOperatorMap(LogicalPlan queryPlan, Map<String, LogicalRelationalOperator> map, Map<String, LOStore> storeMap) {
  // Ibis change : end
        for (Iterator<Operator> it = queryPlan.getOperators(); it.hasNext(); ) {
            LogicalRelationalOperator op = (LogicalRelationalOperator) it.next();
            if (!(op instanceof LOStore)) {
                String alias = op.getAlias();
                if (alias != null) {
                    if (map.containsKey(alias)) throw new RuntimeException("Cannot use Penny with scripts that re-use aliases. This script re-uses alias " + alias + ".");
                    map.put(alias, op);
                }
            }
      // Ibis change : start
      else if (null != storeMap) {
        addStoreOperator(storeMap, (LOStore) op);
      }
      // Ibis change : end
        }
    }

    private Object trace(ClassWithArgs coordinatorClass, Map<String, ClassWithArgs> monitorClasses, String truncateAtAlias, boolean outputToTemp) throws Exception {
        List<String> logicalIds = new ArrayList<String>(monitorClasses.keySet());
        Collections.sort(logicalIds);
        
        LogicalPlan instrumentedQueryPlan = createInstrumentedPlan(monitorClasses, new InetSocketAddress(InetAddress.getLocalHost(), CoordinatorHarness.MASTER_LISTEN_PORT), logicalIds, truncateAtAlias, outputToTemp);
        /* debug: */ //System.out.println("INSTRUMENTED PLAN: " + instrumentedQueryPlan);
        return PigLauncher.launch(pigServer.getPigContext(), instrumentedQueryPlan, coordinatorClass);
    }
    
    public Object trace(ClassWithArgs coordinatorClass, Map<String, ClassWithArgs> monitorClasses, boolean outputToTemp) throws Exception {
        return trace(coordinatorClass, monitorClasses, null, outputToTemp);
    }
    
    public Object truncateAndTrace(ClassWithArgs coordinatorClass, Map<String, ClassWithArgs> monitorClasses, String truncateAtAlias) throws Exception {
        return trace(coordinatorClass, monitorClasses, truncateAtAlias, true);
    }
        
    public Object truncateAndTrace(Class coordinatorClass, Map<String, ClassWithArgs> monitorClasses, String truncateAtAlias) throws Exception {
        return truncateAndTrace(new ClassWithArgs(coordinatorClass), monitorClasses, truncateAtAlias);
    }

    public Object trace(ClassWithArgs coordinatorClass, Map<String, ClassWithArgs> monitorClasses) throws Exception {
        return trace(coordinatorClass, monitorClasses, false);
    }
    
    public Object trace(Class coordinatorClass, Map<String, ClassWithArgs> monitorClasses, boolean outputToTemp) throws Exception {
        return trace(new ClassWithArgs(coordinatorClass), monitorClasses, outputToTemp);
    }

    public Object trace(Class coordinatorClass, Map<String, ClassWithArgs> monitorClasses) throws Exception {
        return trace(coordinatorClass, monitorClasses, false);
    }


  // Ibis change : start
  private static void addStoreOperator(Map<String, LOStore> storeMap, LOStore store) {

    String alias = ((LogicalRelationalOperator)store.getPlan().getPredecessors(store).get(0)).getAlias();

    if (storeMap.containsKey(alias)) throw new RuntimeException("Cannot use Penny with scripts that re-use aliases. This script re-uses alias " + alias + " in store.");
    storeMap.put(alias, store);
  }

  /**
   * Get all the aliases which are 'stored'.
   * TODO: Change from storeMap.keySet() to Collections.unModifiableSet(storeMap.keySet()) so that consumers cant modify it. But I dont know enough of penny to be sure if it is required ...
   *
   * @return
   */
  public Collection<String> getStoreAliases(){
    return storeMap.keySet();
  }
  // Ibis change : end
        /*
     * Get all aliases used in this script.
     */
    public Collection<String> aliases() {
        return aliasOperatorMap.keySet();
    }
    
    public String operator(String alias) {
        return aliasOperatorMap.get(alias).getName();
    }
    
    public int arity(String alias) throws Exception {
        return aliasOperatorMap.get(alias).getSchema().size();
    }
    
  // Ibis change : start

  /**
   * Get the actual pig LogicalRelationalOperator corresponding to an alias.
   * Note that Penny does not support reuse of aliases !
   *
   * @param alias
   * @return
   */
  public LogicalRelationalOperator getLogicalRelationalOperator(String alias){
    return aliasOperatorMap.get(alias);
  }

  /**
   * Get the actual pig LOStore corresponding to an alias which is getting stored.
   * Note that Penny does not support reuse of aliases (even for store) !
   *
   * @param alias
   * @return
   */
  public LOStore getStoreOperator(String alias){
    return storeMap.get(alias);
  }
  // Ibis change : end

    public Map<Integer, List<Integer>> opKeys(String alias) throws Exception {
        LogicalRelationalOperator op = aliasOperatorMap.get(alias);
        
        if (op instanceof LOCogroup) {
            Map<Integer, List<Integer>> opKeys = new HashMap<Integer, List<Integer>>();
            MultiMap<Integer, LogicalExpressionPlan> expressionPlans = ((LOCogroup) op).getExpressionPlans();
            for (int pos : expressionPlans.keySet()) {
                List<Integer> onePosKeys = new LinkedList<Integer>();
                for (LogicalExpressionPlan plan : expressionPlans.get(pos)) {
                    if (plan.size() == 1) {
                        Operator groupExpr = plan.getSources().get(0);
                        if (groupExpr instanceof ConstantExpression) {
                            // "group all" or "group by <const>", so don't append any key col
                        } else if (groupExpr instanceof ProjectExpression) {
                            onePosKeys.add(((ProjectExpression) groupExpr).getColNum());
                        } else {
                            throw new Exception("Penny tagging does not support complex expressions in group/cogroup.");
                        }
                    } else {
                        throw new Exception("Penny tagging does not support complex expressions in group/cogroup.");
                    }
                }
                opKeys.put(pos, onePosKeys);
            }
            return opKeys;
        } else {
            throw new Exception("Operator keys not defined for non-(co)group operator.");
        }
    }
    
    /**
     * Get aliases that this alias depends on directly (for a load, will be an empty list).
     */
    public List<String> inEdges(String alias) {
        List<Operator> inOps = queryPlan.getPredecessors(aliasOperatorMap.get(alias));
        if (inOps == null) inOps = new LinkedList<Operator>();
        List<String> inAliases = new ArrayList<String>(inOps.size());
        for (Operator inOp : inOps) inAliases.add(((LogicalRelationalOperator) inOp).getAlias());
        return inAliases;
    }
    
    /**
     * Get the alias that this alias feeds into directly.
     */
    public String outEdge(String alias) {
        List<Operator> outOps = queryPlan.getSuccessors(aliasOperatorMap.get(alias));
        if (outOps == null) outOps = new LinkedList<Operator>();
        if (outOps.size() > 1) throw new RuntimeException("Expecting at most one outgoing edge.");
        if (outOps.size() == 0) return null;
        if (outOps.get(0).getName().equals("LOStore")) return null;
        else return ((LogicalRelationalOperator) outOps.get(0)).getAlias();
    }
    
    /**
     * Does this script have a linear chain structure (i.e. no joins, splits, etc.)?
     */
    public boolean isChain() {
        for (String alias : aliases()) {
      // Ibis change : start
            if (operator(alias).equals("LOSplit")) return false;
      // Ibis change : end
            if (inEdges(alias).size() > 1) return false;
        }
        return true;
    }
    
    /**
     * Is this alias an entry-point to a new map or reduce task stage?
     */
    public boolean isTaskEntryPoint(String alias) {
        String LogicalRelationalOperator = operator(alias);
        if (LogicalRelationalOperator == null) return false;
    // Ibis change : start
        return (LogicalRelationalOperator.equals("LOLoad") || LogicalRelationalOperator.equals("LOGroup") ||
        LogicalRelationalOperator.equals("LOCogroup") || LogicalRelationalOperator.equals("LOJoin"));
    // Ibis change : end
    }
    
    /**
     * Is this alias an exit-point to for a map or reduce task stage?
     */
    public boolean isTaskExitPoint(String alias) {
        String LogicalRelationalOperator = operator(alias);
    // Ibis change : start
    return (LogicalRelationalOperator.equals("LOStore") || LogicalRelationalOperator.equals("LOGroup") ||
        LogicalRelationalOperator.equals("LOCogroup") || LogicalRelationalOperator.equals("LOJoin"));
    // Ibis change : end
    }
    
    private void truncate(LogicalPlan plan, String alias) throws FrontendException {
        // find truncation point in plan copy
        Operator truncationPoint = null;
        for (Iterator<Operator> it = plan.getOperators(); it.hasNext(); ) {
            LogicalRelationalOperator op = (LogicalRelationalOperator) it.next();
            if (alias.equals(op.getAlias())) {
                truncationPoint = op;
                break;
            }
        }
            
        // find all operators to remove from plan (closure starting at truncation point)
        List<Operator> leads = new LinkedList<Operator>();
        leads.add(truncationPoint);
        Set<Operator> toRemove = new HashSet<Operator>();
        while (!leads.isEmpty()) {
            Operator op = leads.remove(0);
            Collection<Operator> successors = plan.getSuccessors(op);
            if (successors == null) successors = new LinkedList<Operator>();
            toRemove.addAll(successors);
            leads.addAll(successors);
        }
        
        // disconnect operators
        for (Operator op : toRemove) {
            for (Operator pred : new LinkedList<Operator>(plan.getPredecessors(op))) {
                plan.disconnect(pred, op);
            }
        }
        
        // remove operators
        for (Operator op : toRemove) {
            plan.remove(op);
        }
        
        // add "store" operator to store into temp file
        LogicalRelationalOperator store = new LOStore(plan, new FileSpec(getTempFilename(), new FuncSpec("PigStorage")));
        plan.add(store);
        plan.connect(truncationPoint, store);
    }
    
    private LogicalPlan createInstrumentedPlan(Map<String, ClassWithArgs> monitorClasses, InetSocketAddress masterAddr, List<String> logicalIds, String truncateAtAlias, boolean outputToTemp) throws Exception {
        // sanity check
        Set<String> aliases = new HashSet<String>(aliases());
        for (String alias : monitorClasses.keySet()) {
            if (!aliases.contains(alias)) throw new IllegalArgumentException("Illegal request to monitor pig script alias " + alias);
        }
        
        // first, make a fresh copy of the query plan so we can mutate (i.e. instrument) it
        // since LogicalPlan does not offer a deepCopy() method, we'll do this by re-parsing the original script
        ToolsPigServer newPigServer = new ToolsPigServer(pigServer.getPigContext());
        newPigServer.registerNoRun(pigScript, new HashMap<String, String>(), new LinkedList<String>());
        LogicalPlan instrumentedQueryPlan = newPigServer.getPlans().lp;
        if (truncateAtAlias != null) truncate(instrumentedQueryPlan, truncateAtAlias);
        Map<String, LogicalRelationalOperator> cloneAliasOperatorMap = new HashMap<String, LogicalRelationalOperator>();
    // Ibis change : start
        populateAliasOperatorMap(instrumentedQueryPlan, cloneAliasOperatorMap, null);
    // Ibis change : end

        for (String alias : monitorClasses.keySet()) {
            addAgentLogicalRelationalOperator(instrumentedQueryPlan, cloneAliasOperatorMap, alias, monitorClasses.get(alias), masterAddr, logicalIds, withinTaskUpstreamNeighbors(alias), withinTaskDownstreamNeighbors(alias), crossTaskDownstreamNeighbors(alias), incomingCrossTaskKeyFields(alias), outgoingCrossTaskKeyFields(alias));
        }
        
        if (outputToTemp) {
            // convert all store operators to go to temp file
            Collection<LogicalRelationalOperator> ops = new LinkedList<LogicalRelationalOperator>();
            for (Iterator<Operator> it = instrumentedQueryPlan.getOperators(); it.hasNext(); ) {
                ops.add((LogicalRelationalOperator) it.next());
            }
            for (LogicalRelationalOperator op : ops) {
                if (op instanceof LOStore) {
                    FuncSpec funcSpec = ((LOStore) op).getOutputSpec().getFuncSpec();
                    instrumentedQueryPlan.replace(op, new LOStore(instrumentedQueryPlan, new FileSpec(getTempFilename(), funcSpec)));
                }
            }
        }
        
        return instrumentedQueryPlan;
    }
    
    private Set<String> withinTaskUpstreamNeighbors(String alias) {
        if (!isCrossTaskLogicalRelationalOperator(operator(alias))) return new HashSet<String>(inEdges(alias));
        else return new HashSet<String>();
    }
    
    private Set<String> withinTaskDownstreamNeighbors(String alias) {
        Set<String> neighbors = new HashSet<String>();
        for (String otherAlias : aliases()) {
            if (inEdges(otherAlias).contains(alias) && !isCrossTaskLogicalRelationalOperator(operator(otherAlias))) {
                neighbors.add(otherAlias);
            }
        }
        return neighbors;
    }
    
    private Set<String> crossTaskDownstreamNeighbors(String alias) {
        Set<String> neighbors = new HashSet<String>();
        for (String otherAlias : aliases()) {
            if (inEdges(otherAlias).contains(alias) && isCrossTaskLogicalRelationalOperator(operator(otherAlias))) {
                neighbors.add(otherAlias);
            }
        }
        return neighbors;
    }
    
    private Map<String, List<Integer>> incomingCrossTaskKeyFields(String alias) throws Exception {
        if (isCrossTaskLogicalRelationalOperator(operator(alias))) {
            Map<String, List<Integer>> result = new HashMap<String, List<Integer>>();
            int pos = 0;
            for (String originAlias : inEdges(alias)) {
                List<Integer> oneResult = new ArrayList<Integer>();
                if (operator(alias).equals("LOCogroup")) {
                    int numKeys = opKeys(alias).get(pos).size();
                    if (numKeys > 1) {        // (co)group with multiple group keys
                        throw new Exception("Penny tagging does not support complex expressions in group/cogroup.");
                    } else if (numKeys == 1) {                                        // (co)group with single group key
                        oneResult.add(0);
                    } else {            // "group all" or "group <const>"
                        // leave empty
                    }
                } else if (operator(alias).equals("LOSort")) {
                    oneResult.addAll(generateFieldList(0, arity(alias)));                                // for sort, use all fields
                } else if (operator(alias).equals("LOJoin")) {
                    if (pos == 0) oneResult.addAll(generateFieldList(0, arity(originAlias)));            // for join, use all fields from one input
                    else oneResult.addAll(generateFieldList(arity(originAlias), arity(alias)));
                } else {
                    throw new RuntimeException("Unrecognized operator: " + operator(alias));
                }
                result.put(originAlias, oneResult);
                pos++;
            }
            return result;
        } else {
            return null;
        }
    }
    
    private List<Integer> outgoingCrossTaskKeyFields(String alias) throws Exception {
        String dstAlias = outEdge(alias);
        int pos = inEdges(dstAlias).indexOf(alias);
        if (dstAlias != null && isCrossTaskLogicalRelationalOperator(operator(dstAlias))) {
            if (operator(dstAlias).equals("LOCogroup")) {
                return opKeys(dstAlias).get(pos);
            } else if (operator(dstAlias).equals("LOJoin") || operator(dstAlias).equals("LOSort")) {
                return generateFieldList(0, arity(alias));
            } else {
                throw new RuntimeException("Unrecognized operator: " + operator(dstAlias));
            }
        } else {
            return null;
        }
    }
    
    private static List<Integer> generateFieldList(int a, int b) {
        List<Integer> l = new ArrayList<Integer>();
        for (int i = a; i < b; i++) l.add(i);
        return l;
    }
    
    private static boolean isCrossTaskLogicalRelationalOperator(String opName) {
        return (opName.equals("LOCogroup") || opName.equals("LOCross") || opName.equals("LOJoin") || opName.equals("LOSort"));
    }
    
    private static void addAgentLogicalRelationalOperator(LogicalPlan queryPlan, Map<String, LogicalRelationalOperator> aliasOperatorMap, String alias, ClassWithArgs monitorClass, InetSocketAddress masterAddr, List<String> logicalIds, Set<String> withinTaskUpstreamNeighbors, Set<String> withinTaskDownstreamNeighbors, Set<String> crossTaskDownstreamNeighbors, Map<String, List<Integer>> incomingCrossTaskKeyFields, List<Integer> outgoingCrossTaskKeyFields) throws InstantiationException, IllegalAccessException, IOException {
        //String monitorFuncName = "monitor_" + alias;
        MonitorAgentUDFArgs args = new MonitorAgentUDFArgs(alias, monitorClass, masterAddr, logicalIds, withinTaskUpstreamNeighbors, withinTaskDownstreamNeighbors, crossTaskDownstreamNeighbors, incomingCrossTaskKeyFields, outgoingCrossTaskKeyFields);
        
        // create pig LogicalRelationalOperator equivalent to this syntax:
        // "DEFINE " + monitorFuncName + " " + MonitorAgentUDF.class.getCanonicalName() + "(\'" + ObjectSerializer.serialize(args) + "\');
        // alias + " = FOREACH " + alias + " GENERATE FLATTEN(" + monitorFuncName + "(" + monitorFieldList(monitorClass.theClass()) + "));

        FuncSpec funcSpec = new FuncSpec(MonitorAgentUDF.class.getCanonicalName(), ObjectSerializer.serialize(args));
        
        LOForEach agentOp = new LOForEach(queryPlan);
        LogicalPlan innerPlan = new LogicalPlan();
        agentOp.setInnerPlan(innerPlan);
        
        boolean[] flatten = { true };
        List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>(1);
        LogicalRelationalOperator innerLoad = new LOInnerLoad(innerPlan, agentOp, -1);        // -1 in the 3rd arg. means "*"
        LogicalRelationalOperator innerGenerate = new LOGenerate(innerPlan, exps, flatten);
        innerPlan.add(innerLoad);
        innerPlan.add(innerGenerate);
        innerPlan.connect(innerLoad, innerGenerate);

        LogicalExpressionPlan lep = new LogicalExpressionPlan();
        LogicalExpression proj = new ProjectExpression(lep, 0, -1, innerGenerate);    // -1 in the 3rd arg. means "*"
        LogicalExpression func = new UserFuncExpression(lep, funcSpec);
        lep.add(proj);
        lep.add(func);
        lep.connect(func, proj);
        exps.add(lep);
        
        LogicalRelationalOperator opBefore = aliasOperatorMap.get(alias);
        LogicalRelationalOperator opAfter = (LogicalRelationalOperator) queryPlan.getSuccessors(opBefore).get(0);
        queryPlan.add(agentOp);
        queryPlan.insertBetween(opBefore, agentOp, opAfter);
    }
    
    private String monitorFieldList(Class monitorClass) throws InstantiationException, IllegalAccessException {
        Set<Integer> fields = ((MonitorAgent) monitorClass.newInstance()).furnishFieldsToMonitor();
        if (fields == null) {
            return "*";
        } else {
            StringBuffer sb = new StringBuffer();
            
            for (Iterator<Integer> it = fields.iterator(); it.hasNext(); ) {
                sb.append("$" + it.next());
                if (it.hasNext()) sb.append(", ");
            }
            return sb.toString();
        }
    }
    
    private String getTempFilename() {
        return ("/tmp/t_" + Math.abs(rand.nextInt()));
    }
    
    @Override
    public String toString() {
        return queryPlan.toString();
    }

}
