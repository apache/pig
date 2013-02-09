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
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.parser.PigParserNode.InvocationPoint;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

class PigMacro {

    private static final Log LOG = LogFactory.getLog(PigMacro.class);

    private String fileName;
    private String name;
    private String body;
    private List<String> params;
    private List<String> rets;
    private Map<String, PigMacro> seen;
    private Set<String> macroStack;
    private long idx = 0;
    
    // The start line number of this macro in the script
    private int startLine = 0;

    PigMacro(String name, String file, List<String> params,
            List<String> returns, String body, Map<String, PigMacro> seen) {
        this.name = name;
        this.params = (params == null) ? new ArrayList<String>() : params;
        this.rets = (returns == null) ? new ArrayList<String>() : returns;
        this.fileName = file;
        this.body = body;
        this.seen = seen;
        this.macroStack = new HashSet<String>(); 
        LOG.debug("Macro '" + name + "' is defined");
    }
    
    String getName() { return name; }
    
    void setStack(Set<String> stack) {
        macroStack = stack;
    }

    Set<String> getStack() { return macroStack; }
    
    void setStartLine(int start) {
        this.startLine = start;
    }
    
    int getStartLine() {
        return startLine;
    }
    
    private CommonTree inline(String[] inputs, String[] outputs, CommonTree t,
            String file) throws ParserException {
        String in = substituteParams(inputs, outputs, t.getLine(), file);
        
        Set<String> masks = new HashSet<String>();
        if (inputs != null) {
            for (String s : inputs) {
                masks.add(s);
            }
        }
        
        for (String s : outputs) {
            masks.add(s);
        }
 
        return maskAlias(in, masks, t, file);
    }
    
    
    private String substituteParams(String[] inputs, String[] outputs,
            int line, String file) throws ParserException {
        if ((inputs == null && !params.isEmpty())
                || (inputs != null && inputs.length != params.size())) {
            String msg = getErrorMessage(file, line,
                    "Failed to expand macro '" + name + "'",
                    "Expected number of parameters: " + params.size()
                            + " actual number of inputs: "
                            + ((inputs == null) ? 0 : inputs.length));
            throw new ParserException(msg);
        }
        boolean isVoidReturn = false;
        if (rets.isEmpty()) {           
            if (outputs != null && outputs.length > 0) {
                String msg = getErrorMessage(file, line, "Cannot expand macro '"
                        + name + "'",
                        "Expected number of return aliases: 0" 
                                + " actual number of return values: "
                                + outputs.length);
                throw new ParserException(msg);
            }
            isVoidReturn = true;
        } 
        
        if (!isVoidReturn && ((outputs == null && !rets.isEmpty())
                || (outputs != null && outputs.length != rets.size()))) {
            String msg = getErrorMessage(file, line, "Failed to expand macro '"
                    + name + "'",
                    "Expected number of return aliases: " + rets.size()
                            + " actual number of return values: "
                            + ((outputs == null) ? 0 : outputs.length));
            throw new ParserException(msg);
        }
        
        String[] args = new String[params.size()];
        for (int i=0; i<params.size(); i++) {
        	 if (inputs[i].startsWith("$"))
        		 inputs[i]="\\\\"+inputs[i];
            args[i] = params.get(i) + "=" + inputs[i];
        }
        if (!isVoidReturn) {
            String[] args1 = new String[params.size() + rets.size()];
            System.arraycopy(args, 0, args1, 0, params.size());
            args = args1;
            for (int i=0; i<rets.size(); i++) {
                args[params.size() + i] = rets.get(i) + "=" + outputs[i];
            }
        }
        StringWriter writer = new StringWriter();
        BufferedReader in = new BufferedReader(new StringReader(body));
        try {
            ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(
                    50);
            psp.genSubstitutedFile(in, writer, args, null);
        } catch (Exception e) {
            // catch both ParserException and RuntimeException
            String msg = getErrorMessage(file, line,
                    "Macro inline failed for macro '" + name + "'",
                    e.getMessage() + "\n Macro content: " + body);
            throw new ParserException(msg);
        } 
        
        LOG.debug("--- after substition:\n" + writer.toString());
        
        return writer.toString();
    }
        
    private CommonTree maskAlias(String in, Set<String> masks, CommonTree tree,
            String file) throws ParserException {
        
        // this is the MACRO_INLINE node. the real line number is in the 
        // macro name node
        int line = tree.getChild(0).getLine();

        CharStream input = null;
        try {
            // parse macro body into ast 
            input = new QueryParserStringStream(in, file);
        } catch (IOException e) {
            String msg = getErrorMessage(file, line, "Failed to inline macro '"
                    + name + "'", e.getMessage() + "\nmacro content: " + in);
            throw new ParserException(msg);
        }
        
        QueryLexer lex = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lex);
            
        QueryParser.query_return result = null;
        QueryParser parser = QueryParserUtils.createParser(tokens, startLine-1);

        try {
            result = parser.query();
        } catch (RecognitionException e) {
            String msg = (fileName == null) ? parser.getErrorHeader(e)
                    : QueryParserUtils.generateErrorHeader(e, fileName);
            msg += " " + parser.getErrorMessage(e, parser.getTokenNames());
            String msg2 = getErrorMessage(file, line, "Failed to parse macro '"
                    + name + "'", msg + "\nmacro content: " + in);
            throw new ParserException(msg2);
        }

        CommonTree ast = (CommonTree)result.getTree();
        
        LOG.debug("AST for macro '" + name + "':\n" + ast.toStringTree());
            
        List<CommonTree> macroDefNodes = new ArrayList<CommonTree>();
        traverseMacro(ast, macroDefNodes, "MACRO_DEF");
        if (!macroDefNodes.isEmpty()) {
            String fname = ((PigParserNode)ast).getFileName();
            String msg = getErrorMessage(fname, ast.getLine(),
                    "Invalide macro definition", "macro '" + name
                            + "' contains macro definition.\nmacro content: "
                            + body);
            throw new ParserException(msg);
        }
        
        // add macro invocation points to the expanded macro tree
        PigParserNode pnode = (PigParserNode)tree;
        List<InvocationPoint> invStack = pnode.getInvocationStack();
        List<InvocationPoint> newInvStack = (invStack == null) ? new ArrayList<InvocationPoint>()
                : new ArrayList<InvocationPoint>(invStack);

        InvocationPoint pt = new InvocationPoint(line, file, name);
        newInvStack.add(pt);
        setInvocationStack(ast, newInvStack);
        
        // recursively expand the inline macros
        List<CommonTree> inlineNodes = new ArrayList<CommonTree>();
        traverseMacro(ast, inlineNodes, "MACRO_INLINE");

        for (CommonTree t : inlineNodes) {
            CommonTree newTree = macroInline(t,
                    new ArrayList<PigMacro>(seen.values()), macroStack);
            QueryParserUtils.replaceNodeWithNodeList(t, newTree, null);
        }
        
        // mask the aliases in the inlined macro 
        CommonTreeNodeStream nodes = new CommonTreeNodeStream(ast);
        AliasMasker walker = new AliasMasker(nodes);
        walker.setParams(masks, name, idx++);

        AliasMasker.query_return result2 = null;
        CommonTree commonTree = null;
        
        try {
            result2 = walker.query();
        } catch (RecognitionException e) {
            String msg = walker.getErrorHeader(e) + " "
                    + walker.getErrorMessage(e, walker.getTokenNames());
            String msg2 = getErrorMessage(file, line, "Failed to mask macro '"
                    + name + "'", msg + "\nmacro content: " + in);
            throw new ParserException(msg2);
        }
        
        commonTree = result2.tree;

        LOG.debug("AST for masked macro '" + name + "':\n"
                + commonTree.toStringTree());
 
        return commonTree;
    }
    
    private static void setInvocationStack(Tree ast, List<InvocationPoint> stack) {        
        PigParserNode node = (PigParserNode)ast;
        node.setInvocationStack(stack);
        int n = node.getChildCount();
        for (int i = 0; i < n; i++) {
            setInvocationStack(node.getChild(i), stack);
        }
    }
    
    /*
     * Validates that return alias exists in the macro body.
     */
    void validate() throws IOException {
        if (rets.isEmpty()) {
            return;
        }
        
        HashSet<String> testSet = new HashSet<String>();
        
        StreamTokenizer st   = new StreamTokenizer(new StringReader(body));

        st.wordChars('.', '.');
        st.wordChars('0', '9');
        st.wordChars('_', '_');
        st.wordChars('$', '$');
        st.lowerCaseMode(false);
        st.ordinaryChar('/');
        st.slashStarComments(true);
        
        while (st.nextToken() != StreamTokenizer.TT_EOF) {
            if (matchWord(st, "define", false) && matchDollarAlias(st, true)) {
                testSet.add(st.sval.substring(1));
            } else if (matchDollarAlias(st, false)) {
                String prevWord = st.sval;
                if (matchWord(st, "if", true)) {
                    testSet.add(prevWord.substring(1));
                } else if (matchChar(st, '=', true) && !matchChar(st, '=', true)) {
                     testSet.add(prevWord.substring(1)); 
                } else if (matchChar(st, ',', true)) {
                    // possible mult-alias inlining of a macro
                    ArrayList<String> mlist = new ArrayList<String>();
                    mlist.add(prevWord);
                    if (isMultiValueReturn(st, mlist, true)) {    
                        for (String s : mlist) {
                            testSet.add(s.substring(1));
                        }
                    } 
                } 
            } else if (matchChar(st, '-', false) && matchChar(st, '-', true)) {
                skipSingleLineComment(st);
            }
        }
        
        for (String s : rets) {
            if (!testSet.contains(s)) {
                throw new IOException("Macro '" + name
                        + "' missing return alias: " + s);
            }
        }
    }
    
    // check for multi-value return pattern: alias, alias, ..., alias =
    private static boolean isMultiValueReturn(StreamTokenizer st,
            List<String> mlist, boolean comma) throws IOException {
        int lookahead = st.nextToken();
        if ((comma && lookahead == StreamTokenizer.TT_WORD)
                || (!comma && matchChar(st, ',', false))) {
            if (matchDollarAlias(st, false)) {
                mlist.add(st.sval);
            }
            return isMultiValueReturn(st, mlist, !comma);
        }
        if (!comma && lookahead == '=' && !matchChar(st, '=', true)) {
            return true;
        }
        return false;
    }
        
    private static boolean matchDollarAlias(StreamTokenizer st, boolean next)
            throws IOException {
        int type = next ? st.nextToken() : st.ttype;
        if (type == StreamTokenizer.TT_WORD && st.sval.charAt(0) == '$'
                && st.sval.length() > 1) {
            return true;
        }
        if (next) st.pushBack();
        return false;
    }

    private static boolean matchWord(StreamTokenizer st, String word,
            boolean next) throws IOException {
        int type = next ? st.nextToken() : st.ttype;
        if (type == StreamTokenizer.TT_WORD
                && st.sval.equalsIgnoreCase(word)) {
            return true;
        }
        if (next) st.pushBack();
 
        return false;
    }
    
    private static boolean matchChar(StreamTokenizer st, int c, boolean next)
            throws IOException {
        int type = next ? st.nextToken() : st.ttype;
        if (type == c) return true;
        if (next) st.pushBack();
        return false;
    }

    private static void skipSingleLineComment(StreamTokenizer st)
            throws IOException {
        int lineNo = st.lineno();
        int lookahead = st.nextToken();
        while (lookahead != StreamTokenizer.TT_EOF && lookahead != '\n') {
            if (st.lineno() > lineNo) break;
            lookahead = st.nextToken();
        }
        st.pushBack();
    }
    
    private static void traverseMacro(Tree t, List<CommonTree> nodes,
            String nodeType) {
        if (t.getText().equals(nodeType)) {
            nodes.add((CommonTree) t);
        }
        int n = t.getChildCount();
        for (int i = 0; i < n; i++) {
            Tree t0 = t.getChild(i);
            traverseMacro(t0, nodes, nodeType);
        }
    }
     
    /*
     * Macro inline nodes have the following form:
     * 
     * (MACRO_INLINE <name> (RETURN_VAL <values>) (PARAMS <values>)) 
     * 
     * Child nodes:
     *      0: macro name
     *      1: list of return values
     *      2: list of parameters
     */
    static CommonTree macroInline(CommonTree t, List<PigMacro> macroDefs, Set<String> macroStack)
            throws ParserException {
        // get name
        String mn = t.getChild(0).getText();

        // get macroDef
        PigMacro macro = null;
        for (PigMacro pm : macroDefs) {
            if (pm.getName().equals(mn)) {
                macro = pm;
                break;
            }
        }

        String file = ((PigParserNode)t).getFileName();
        
        if (macro == null) {
            String msg = getErrorMessage(file, t.getLine(),
                    "Cannot expand macro '" + mn + "'",
                    "Macro must be defined before expansion.");
            throw new ParserException(msg);
        }

        if (macroStack.contains(macro.name)) {
            String msg = getErrorMessage(file, t.getLine(),
                    "Cannot expand macro '" + mn + "'",
                      "Macro can't be defined circularly.");
            throw new ParserException(msg);
        }
        
        // set nested macro call stack
        Set<String> newStack = new HashSet<String>(macroStack);
        newStack.add(macro.name);
        macro.setStack(newStack);
        
        // get return values
        int n = t.getChild(1).getChildCount();
        String[] rets = new String[n];
        for (int i = 0; i < n; i++) {
            rets[i] = t.getChild(1).getChild(i).getText();
        }

        // get parameters
        int m = t.getChild(2).getChildCount();
        String[] params = new String[m];
        for (int i = 0; i < m; i++) {
            params[i] = t.getChild(2).getChild(i).getText();
        }

        return macro.inline(params, rets, t, file);
    }
  
    private static String getErrorMessage(String file, int line, String header,
            String reason) {
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        if (file != null) {
            sb.append("file ").append(file).append(", ");
        }
        sb.append("line ").append(line).append("> ").append(header);
        if (reason != null) {
            sb.append(". Reason: ").append(reason);
        }
        return sb.toString();
    }
}
