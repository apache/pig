/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.evaluation.xml;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * XPathAll is a function that allows for Tuple extraction from xml
 */
public class XPathAll extends EvalFunc<Tuple> {

    private javax.xml.xpath.XPath xmlPath = null;
    private String xml = null;
    private Document document;

    /**
     * Caching of the xpath & xml in case the next call to xpath() is feeding
     * the same xml document The reason for this is because creating an xpath
     * object is costly.
     */
    private static boolean cache = true;
    private static boolean ignoreNamespace = true;

    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    public static enum ARGUMENTS {
        XML_FILE(0), XPATH(1), CACHE(2), IGNORE_NAMESPACE(3);

        private int argument;

        ARGUMENTS(int argument) {
            this.argument = argument;
        }

        int getPosition() {
            return this.argument;
        }
    }

    public static final String EMPTY_STRING = "";

    /**
     * input should contain: 1) xml 2) xpath 3) optional cache xml doc flag 4)
     * optional ignore namespace flag
     * 
     * The optional fourth parameter (IGNORE_NAMESPACE), if set true will remove
     * the namespace from xPath For example xpath /html:body/html:div will be
     * considered as /body/div
     * 
     * Usage: 	1) XPathAll(xml, xpath) 
     * 			2) XPathAll(xml, xpath, false)
     * 			3) XPathAll(xml, xpath, false, false)
     * 
     * @param input
     * 		  1st element should to be the xml 2nd element should be the xpath
     *        3rd optional boolean cache flag (default true) 
     *        4th optional boolean ignore namespace flag(default true)
     * 
     *        This UDF will cache the last xml document. This is helpful when
     *        multiple consecutive xpathAll calls are made for the same xml
     *        document. Caching can be turned off to ensure that the UDF's
     *        recreates the internal javax.xml.xpath.XPathAll for every call
     *        
     *        This UDF will also support ignoring the namespace in the xml tags.
     *        This will help to search xpath items by ignoring its namespace.
     *        Ignoring of the namespace can be turned off for special cases using
     *        a fourth argument in the UDF. 
     *        
     * 
     * @return Tuple result or null if no match
     */
    @Override
    public Tuple exec(final Tuple input) throws IOException {

        if (!isArgsValid(input)) { // Validate arguments
            return null;
        }

        try {
        	
            final String xml = (String) input.get(ARGUMENTS.XML_FILE.getPosition());
            if (xml == null) {
                warn("Error processing input, invalid parameter" + input, PigWarning.UDF_WARNING_1);
                return null;
            }

            if (input.size() > 2) {
                cache = (Boolean) input.get(ARGUMENTS.CACHE.getPosition());
            }

            if (input.size() > 3) {
                ignoreNamespace = (Boolean) input.get(ARGUMENTS.IGNORE_NAMESPACE.getPosition());
            }

            // Process XML
            if (!cache || xmlPath == null || !xml.equals(this.xml)) { // Cache verification
                final InputSource source = new InputSource(new StringReader(xml));

                this.xml = xml; // track the xml for subsequent calls to this udf

                final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                final DocumentBuilder db = dbf.newDocumentBuilder();

                this.document = db.parse(source);

                final XPathFactory xpathFactory = XPathFactory.newInstance();
                this.xmlPath = xpathFactory.newXPath();
            }

            String xpathString = (String) input.get(ARGUMENTS.XPATH.getPosition());

            if (ignoreNamespace) {
                xpathString = createNameSpaceIgnoreXpathString(xpathString);
            }

            final NodeList nodeEntries = (NodeList) xmlPath.compile(xpathString).evaluate(document,
                    XPathConstants.NODESET);

            if (nodeEntries == null) {
                return null;
            }

            Tuple resultTuple = tupleFactory.newTuple(nodeEntries.getLength());

            for (int nodeEntryIndex = 0; nodeEntryIndex < nodeEntries.getLength(); nodeEntryIndex++) {
                final String ELEMENT_NODE_SEPARATOR = ", ";

                Node node = nodeEntries.item(nodeEntryIndex);

                // Parse the Node
                final NodeList childNodes = node.getChildNodes();
                if (childNodes == null) {
                    continue;
                }

                String nodeData = "";
                boolean dataFlag = false;

                for (int i = 0; i < childNodes.getLength(); i++) {
                    try {
                        Node subNode = childNodes.item(i);
                        if (subNode.getNodeType() == Node.ELEMENT_NODE) {
                            if (subNode.getFirstChild().getNodeValue() == null) {
                                // If There is no direct element, return blank
                                nodeData = nodeData.concat(ELEMENT_NODE_SEPARATOR);
                                nodeData = nodeData.concat(EMPTY_STRING);
                                dataFlag = true;
                                continue;
                            }
                            nodeData = nodeData.concat(ELEMENT_NODE_SEPARATOR);
                            nodeData = nodeData.concat(subNode.getFirstChild().getNodeValue());
                            dataFlag = true;
                        } else if (subNode.getNodeType() == Node.TEXT_NODE
                                || subNode.getNodeType() == Node.ATTRIBUTE_NODE) {
                            nodeData = nodeData.concat(ELEMENT_NODE_SEPARATOR);
                            nodeData = nodeData.concat(subNode.getNodeValue());
                            dataFlag = true;
                        }
                    } catch (Exception ex) {
                        continue;
                    }
                }

                if (dataFlag) {
                    nodeData = nodeData.replaceFirst(ELEMENT_NODE_SEPARATOR, EMPTY_STRING);
                    resultTuple.set(nodeEntryIndex, nodeData);
                }
            }
            return resultTuple;

        } catch (Exception e) {
            warn("Error processing input " + input.getType(0), PigWarning.UDF_WARNING_1);
            return null;
        }
    }
    
    
    /**
     * Validates values of the input parameters.
     * 
     * @param Tuple
     * @return boolean
     */
    private boolean isArgsValid(final Tuple input) {
        if (input == null || input.size() <= 1) {
            warn("Error processing input, not enough parameters or null input" + input, PigWarning.UDF_WARNING_1);
            return false;
        }

        if (input.size() > 4) {
            warn("Error processing input, too many parameters" + input, PigWarning.UDF_WARNING_1);
            return false;
        }

        try {
            // 3rd Parameter - CACHE
            if (input.size() > 2 && !(input.get(ARGUMENTS.CACHE.getPosition()) instanceof Boolean)) {
                warn("Error processing input, invalid value in 3rd parameter" + input, PigWarning.UDF_WARNING_1);
                return false;
            }

            // 4rd Parameter IGNORE_NAMESPACE
            if (input.size() > 3 && !(input.get(ARGUMENTS.IGNORE_NAMESPACE.getPosition()) instanceof Boolean)) {
                warn("Error processing input, invalid value in 4th parameter" + input, PigWarning.UDF_WARNING_1);
                return false;
            }
        } catch (Exception ex) {
            return false;
        }

        return true;
    }

    /**
     * Returns a new the xPathString by adding additional parameters 
     * in the existing xPathString for ignoring the namespace during compilation.
     * 
     * @param String xpathString
     * @return String modified xpathString
     */
    private String createNameSpaceIgnoreXpathString(final String xpathString) {
        final String QUERY_PREFIX = "//*";
        final String LOCAL_PREFIX = "[local-name()='";
        final String LOCAL_POSTFIX = "']";
        final String SPLITTER = "/";

        try {
            String xpathStringWithLocalName = EMPTY_STRING;
            String[] individualNodes = xpathString.split(SPLITTER);

            for (String node : individualNodes) {
                xpathStringWithLocalName = xpathStringWithLocalName.concat(QUERY_PREFIX + LOCAL_PREFIX + node
                        + LOCAL_POSTFIX);
            }

            return xpathStringWithLocalName;
        } catch (Exception ex) {
            return xpathString;
        }
    }

    /**
     * Returns argument schemas of the UDF.
     * 
     * @return List
     */
    
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {

        final List<FuncSpec> funcList = new ArrayList<FuncSpec>();

        /* either two chararray arguments */
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema twoArgInSchema = new Schema(fields);

        funcList.add(new FuncSpec(this.getClass().getName(), twoArgInSchema));

        /* or two chararray and a boolean argument */
        fields = new ArrayList<FieldSchema>();
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.BOOLEAN));

        Schema threeArgInSchema = new Schema(fields);

        funcList.add(new FuncSpec(this.getClass().getName(), threeArgInSchema));

        /* or two chararray and two boolean arguments */
        fields = new ArrayList<FieldSchema>();
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.BOOLEAN));
        fields.add(new Schema.FieldSchema(null, DataType.BOOLEAN));

        Schema fourArgInSchema = new Schema(fields);

        funcList.add(new FuncSpec(this.getClass().getName(), fourArgInSchema));

        return funcList;
    }

}
