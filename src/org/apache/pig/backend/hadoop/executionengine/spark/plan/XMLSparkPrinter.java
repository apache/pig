/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.backend.hadoop.executionengine.spark.plan;

import java.io.PrintStream;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.XMLPhysicalPlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static javax.xml.transform.OutputKeys.INDENT;
import static javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION;


public class XMLSparkPrinter extends SparkOpPlanVisitor {

    private PrintStream mStream = null;

    private Document doc = null;
    private Element root = null;

    public XMLSparkPrinter(PrintStream ps, SparkOperPlan plan) throws ParserConfigurationException {
        super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
        mStream = ps;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        this.doc = builder.newDocument();
        this.root = this.doc.createElement("sparkPlan");
        this.doc.appendChild(this.root);

    }


    public void closePlan() throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

        StringWriter sw = new StringWriter();
        StreamResult result = new StreamResult(sw);
        DOMSource source = new DOMSource(doc);
        transformer.transform(source, result);
        mStream.println(sw.toString());
    }

    @Override
    public void visitSparkOp(SparkOperator so) throws VisitorException {
        Element sparkNode = doc.createElement("sparkNode");
        sparkNode.setAttribute("scope", "" + so.getOperatorKey().id);
        if(so instanceof NativeSparkOperator) {
            Element nativeSparkOper = doc.createElement("nativeSpark");
            nativeSparkOper.setTextContent(((NativeSparkOperator)so).getCommandString());
            sparkNode.appendChild(nativeSparkOper);
            root.appendChild(sparkNode);
            return;
        }

        if (so.physicalPlan != null && so.physicalPlan.size() > 0) {
            XMLPhysicalPlanPrinter<PhysicalPlan> printer = new XMLPhysicalPlanPrinter<>(so.physicalPlan, doc, sparkNode);
            printer.visit();
        }

        root.appendChild(sparkNode);

    }

}
