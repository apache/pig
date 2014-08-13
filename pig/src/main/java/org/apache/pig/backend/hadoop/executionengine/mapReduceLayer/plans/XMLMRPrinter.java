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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.io.PrintStream;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.XMLPhysicalPlanPrinter;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class XMLMRPrinter extends MROpPlanVisitor {

    private PrintStream mStream = null;

    private Document doc = null;
    private Element root = null;
 
    
    /**
     * @param ps PrintStream to output plan information to
     * @param plan MR plan to print
     * @throws ParserConfigurationException 
     */
    public XMLMRPrinter(PrintStream ps, MROperPlan plan) throws ParserConfigurationException {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        mStream = ps;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.doc = builder.newDocument();
        this.root = this.doc.createElement("mapReducePlan");
        this.doc.appendChild(this.root);
    }
    
    public void closePlan() throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        
        StringWriter sw = new StringWriter();
        StreamResult result = new StreamResult(sw);
        DOMSource source = new DOMSource(doc);
        transformer.transform(source, result);
        mStream.println(sw.toString());
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        Element mrNode = doc.createElement("mapReduceNode");
        mrNode.setAttribute("scope", "" + mr.getOperatorKey().id);
        if(mr instanceof NativeMapReduceOper) {
            Element nativeMROper = doc.createElement("nativeMapReduce");
            nativeMROper.setTextContent(((NativeMapReduceOper)mr).getCommandString());
            mrNode.appendChild(nativeMROper);
            root.appendChild(mrNode);
            return;
        }
        if (mr.mapPlan != null && mr.mapPlan.size() > 0) {
            Element mrPlanNode = doc.createElement("map");
            XMLPhysicalPlanPrinter<PhysicalPlan> printer = new XMLPhysicalPlanPrinter<PhysicalPlan>(mr.mapPlan, doc, mrPlanNode);
            printer.visit();
            mrNode.appendChild(mrPlanNode);
        }
        if (mr.combinePlan != null && mr.combinePlan.size() > 0) {
            Element cPlanNode = doc.createElement("combine");
            XMLPhysicalPlanPrinter<PhysicalPlan> printer = new XMLPhysicalPlanPrinter<PhysicalPlan>(mr.combinePlan, doc, cPlanNode);
            printer.visit();
            mrNode.appendChild(cPlanNode);
        }
        if (mr.reducePlan != null && mr.reducePlan.size() > 0) {
            Element rPlanNode = doc.createElement("reduce");
            XMLPhysicalPlanPrinter<PhysicalPlan> printer = new XMLPhysicalPlanPrinter<PhysicalPlan>(mr.reducePlan, doc, rPlanNode);
            printer.visit();
            mrNode.appendChild(rPlanNode);
        }
        root.appendChild(mrNode);
    }
}