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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


public class XMLPhysicalPlanPrinter<P extends OperatorPlan<PhysicalOperator>> extends
        PhyPlanVisitor {

    private Document doc = null;
    private Element parent = null;

    public XMLPhysicalPlanPrinter(PhysicalPlan plan, Document doc, Element parent) {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        this.doc = doc;
        this.parent = parent;
    }

    @Override
    public void visit() throws VisitorException {
        try {
            depthFirstPP(parent);
        } catch (IOException ioe) {
            int errCode = 2079;
            String msg = "Unexpected error while printing physical plan.";
            throw new VisitorException(msg, errCode, PigException.BUG, ioe);
        }
    }

    public void print(OutputStream printer) throws VisitorException, IOException {
        TransformerFactory factory = TransformerFactory.newInstance();
        try {
            Transformer transformer = factory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            StringWriter sw = new StringWriter();
            StreamResult result = new StreamResult(sw);
            DOMSource source = new DOMSource(doc);
            transformer.transform(source, result);
            printer.write(sw.toString().getBytes("UTF-8"));
        } catch (TransformerException e) {
            e.printStackTrace();
        }
    }

    private Element createAlias(PhysicalOperator po) {
        Element aliasNode = null;
        String alias = po.getAlias();
        if (alias != null) {
            aliasNode = doc.createElement("alias");
            aliasNode.setTextContent(alias);
        }
        return aliasNode;
    }

    protected void depthFirstPP(Element parentNode) throws VisitorException {
        List<PhysicalOperator> leaves = mPlan.getLeaves();
        Collections.sort(leaves);
        for (PhysicalOperator leaf : leaves) {
            depthFirst(leaf, parentNode);
        }
    }


    private void visitPlan(PhysicalPlan pp, Element parentNode) throws VisitorException {
        if(pp!=null) {
            XMLPhysicalPlanPrinter<PhysicalPlan> ppp =
                    new XMLPhysicalPlanPrinter<PhysicalPlan>(pp, doc, parentNode);
            ppp.visit();
        }
    }


    private void visitPlan(List<PhysicalPlan> lep, Element parentNode) throws VisitorException {
        if(lep!=null)
            for (PhysicalPlan ep : lep) {
                visitPlan(ep, parentNode);
            }
    }

    private Element createPONode(PhysicalOperator node) {
        Element PONode = doc.createElement(node.getClass().getSimpleName());
        PONode.setAttribute("scope", "" + node.getOperatorKey().id);
        Element alias = createAlias(node);
        if (alias != null) {
            PONode.appendChild(alias);
        }
        if (node instanceof POStore) {
            Element storeFile = doc.createElement("storeFile");
            storeFile.setTextContent(((POStore)node).getSFile().getFileName());
            PONode.appendChild(storeFile);

            Element isTmpStore = doc.createElement("isTmpStore");
            isTmpStore.setTextContent(Boolean.valueOf(((POStore)node).isTmpStore()).toString());
            PONode.appendChild(isTmpStore);
        }
        if (node instanceof POLoad) {
            Element loadFile = doc.createElement("loadFile");
            loadFile.setTextContent(((POLoad)node).getLFile().getFileName());
            PONode.appendChild(loadFile);

            Element isTmpLoad = doc.createElement("isTmpLoad");
            isTmpLoad.setTextContent(Boolean.valueOf(((POLoad)node).isTmpLoad()).toString());
            PONode.appendChild(isTmpLoad);
        }
        return PONode;
    }


    private void depthFirst(PhysicalOperator node, Element parentNode) throws VisitorException {
        Element childNode = null;

        List<PhysicalPlan> subPlans = new ArrayList<PhysicalPlan>();
        if(node instanceof POFilter){
            subPlans.add(((POFilter) node).getPlan());
        } else if(node instanceof POLocalRearrange){
            subPlans = ((POLocalRearrange)node).getPlans();
        } else if(node instanceof POCollectedGroup){
            subPlans = ((POCollectedGroup)node).getPlans();
        } else if(node instanceof POSort){
            subPlans = ((POSort)node).getSortPlans();
        }  else if(node instanceof POForEach){
            subPlans = ((POForEach)node).getInputPlans();
        } else if (node instanceof POSplit) {
            subPlans = ((POSplit)node).getPlans();
        } else if (node instanceof PODemux) {
            subPlans = ((PODemux)node).getPlans();
        } else if(node instanceof POFRJoin){
            childNode = createPONode(node);
            POFRJoin frj = (POFRJoin)node;
            List<List<PhysicalPlan>> joinPlans = frj.getJoinPlans();
            if(joinPlans!=null) {
                for (List<PhysicalPlan> list : joinPlans) {
                    visitPlan(list, childNode);
                }
            }
        } else if(node instanceof POSkewedJoin){
            childNode = createPONode(node);
            POSkewedJoin skewed = (POSkewedJoin)node;
            MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = skewed.getJoinPlans();
            if(joinPlans!=null) {
                List<PhysicalPlan> inner_plans = new ArrayList<PhysicalPlan>();
                inner_plans.addAll(joinPlans.values());
                visitPlan(inner_plans, childNode);
            }
        }

        if (childNode == null) {
            childNode = createPONode(node);
            if (subPlans.size() > 0) {
                visitPlan(subPlans, childNode);
            }
        }
        parentNode.appendChild(childNode);

        List<PhysicalOperator> originalPredecessors = mPlan.getPredecessors(node);
        if (originalPredecessors == null) {
            return;
        }

        List<PhysicalOperator> predecessors =  new ArrayList<PhysicalOperator>(originalPredecessors);

        Collections.sort(predecessors);
        for (PhysicalOperator pred : predecessors) {
            depthFirst(pred, childNode);
        }
    }
}
