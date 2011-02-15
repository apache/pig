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
package org.apache.pig.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;

public class PigMacro {

    private static final Log LOG = LogFactory.getLog(PigMacro.class);

    private String name;
    private String body;
    private List<String> params;
    private List<String> rets;
    private Map<String, PigMacro> seen;
    private long idx = 0;

    public PigMacro(String name) {
        this.name = name;
        this.params = new ArrayList<String>();
        this.rets = new ArrayList<String>();
        LOG.info("Macro '" + name + "' is defined");
    }

    public void setBody(String body, Map<String, PigMacro> seen) {
        this.body = body;
        this.seen = new HashMap<String, PigMacro>(seen);
        expandBody();
    }
    
    public void addParam(String param) {
        params.add(param);
    }

    public void addReturn(String ret) {
        rets.add(ret);
    }

    public String getName() { return name; }

    public String getBody() { return body; }

    public List<String> getParams() { return params; }

    public List<String> getReturns() { return rets; }

    public String inline(String[] inputs, String[] outputs) {
        String in = substituteParams(inputs, outputs);
        Set<String> masks = new HashSet<String>();
        if (inputs != null) {
            for (String s : inputs) {
                masks.add(s);
            }
        }
        for (String s : outputs) {
            masks.add(s);
        }
        return maskAlias(in, masks);
    }
    
    public String substituteParams(String[] inputs, String[] outputs) {
        if ((inputs == null && !params.isEmpty())
                || (inputs != null && inputs.length != params.size())) {
            throw new RuntimeException("Failed to expand macro '" + name
                    + "': expected number of parameters: " + params.size()
                    + " actual number of inputs: "
                    + ((inputs == null) ? 0 : inputs.length));
        }
        if (outputs == null || outputs.length != rets.size()) {
            throw new RuntimeException("Failed to expand macro '" + name
                    + "': expected number of return aliases: " + rets.size()
                    + " actual number of return values: "
                    + ((outputs == null) ? 0 : outputs.length));
        }
        
        String[] args = new String[params.size() + rets.size()];
        for (int i=0; i<params.size(); i++) {
            String p = inputs[i];
            p = p.startsWith("$") ? ("\\\\" + p) : p;
            args[i] = params.get(i) + "=" + p;
        }
        for (int i=0; i<rets.size(); i++) {
            String p = outputs[i];
            p = p.startsWith("$") ? ("\\\\" + p) : p;
            args[params.size() + i] = rets.get(i) + "=" + p;
        }
        StringWriter writer = new StringWriter();
        BufferedReader in = new BufferedReader(new StringReader(body));
        try {
            ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(
                    50);
            psp.genSubstitutedFile(in, writer, args, null);
        } catch (ParseException e) {
            throw new RuntimeException(
                    "Parameter substitution failed for macro " + name, e);
        }
        
        LOG.debug("--- after substition:\n" + writer.toString());
        
        return writer.toString();
    }
    
    public String maskAlias(String in, Set<String> masks) {
        String resultString = "";
        try {
            CharStream input = new QueryParserStringStream(in);
            QueryLexer lex = new QueryLexer(input);
            CommonTokenStream tokens = new  CommonTokenStream(lex);

            QueryParser parser = new QueryParser(tokens);
            QueryParser.query_return result = parser.query();

            Tree ast = (Tree)result.getTree();
            
            LOG.debug(ast.toStringTree());

            CommonTreeNodeStream nodes = new CommonTreeNodeStream(ast);
            AliasMasker walker = new AliasMasker(nodes);
            walker.setParams(masks, name, idx++);

            walker.query();

            LOG.debug("--- walk: \n" + walker.getResult());

            resultString = walker.getResult();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Query parsing failed for macro " + name, e);
        }

        return resultString;
    }
    
    private void expandBody() {
        // expand macros
        boolean done = false;
        
        while (!done) {
            StringReader srd = new StringReader(body);
            ANTLRReaderStream input;
            try {
                input = new ANTLRReaderStream(srd);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read ", e);
            }
            MacroRecursion expander = new MacroRecursion(input);
            expander.setMacros(seen);
            Token token = Token.EOF_TOKEN;
            while ((token = expander.nextToken()) != Token.EOF_TOKEN);
        
            body = expander.getResultString();
            done = !expander.isExpanded();
        }
    }
}
