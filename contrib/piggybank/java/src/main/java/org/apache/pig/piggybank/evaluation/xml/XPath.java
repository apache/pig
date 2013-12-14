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
import javax.xml.xpath.XPathFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * XPath is a function that allows for text extraction from xml
 */
public class XPath extends EvalFunc<String> {

    /** Hold onto last xpath & xml in case the next call to xpath() is feeding the same xml document
     * The reason for this is because creating an xpath object is costly. */
    private javax.xml.xpath.XPath xpath = null;
    private String xml = null;
    private Document document;
    
    private static boolean cache = true;
    
    /**
     * input should contain: 1) xml 2) xpath 3) optional cache xml doc flag
     * 
     * Usage:
     * 1) XPath(xml, xpath)
     * 2) XPath(xml, xpath, false) 
     * 
     * @param 1st element should to be the xml
     *        2nd element should be the xpath
     *        3rd optional boolean cache flag (default true)
     *        
     * This UDF will cache the last xml document. This is helpful when multiple consecutive xpath calls are made for the same xml document.
     * Caching can be turned off to ensure that the UDF's recreates the internal javax.xml.xpath.XPath for every call
     * 
     * @return chararrary result or null if no match
     */
    @Override
    public String exec(final Tuple input) throws IOException {

        if (input == null || input.size() <= 1) {
            warn("Error processing input, not enough parameters or null input" + input,
                    PigWarning.UDF_WARNING_1);
            return null;
        }


        if (input.size() > 3) {
            warn("Error processing input, too many parameters" + input,
                    PigWarning.UDF_WARNING_1);
            return null;
        }

        try {

            final String xml = (String) input.get(0);
            
            if(input.size() > 2)
                cache = (Boolean) input.get(2);
            
            if(!cache || xpath == null || !xml.equals(this.xml))
            {
                final InputSource source = new InputSource(new StringReader(xml));
                
                this.xml = xml; //track the xml for subsequent calls to this udf

                final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                final DocumentBuilder db = dbf.newDocumentBuilder();
                
                this.document = db.parse(source);

                final XPathFactory xpathFactory = XPathFactory.newInstance();

                this.xpath = xpathFactory.newXPath();
                
            }
            
            final String xpathString = (String) input.get(1);

            final String value = xpath.evaluate(xpathString, document);

            return value;

        } catch (Exception e) {
            warn("Error processing input " + input.getType(0), 
                    PigWarning.UDF_WARNING_1);
            
            return null;
        }
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        
        final List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        
        return funcList;
    }

}

