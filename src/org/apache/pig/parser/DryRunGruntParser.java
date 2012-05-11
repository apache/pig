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

import java.io.IOException;
import java.io.Reader;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;

public class DryRunGruntParser extends PigScriptParser {
    
    private StringBuilder sb = new StringBuilder();
    
    private PigContext context;
    
    private Tree parserTree;
    
    private String source;
    
    private int toSkip = 0;
    
    private boolean done = false;
    
    private boolean hasMacro = false;
    
    public DryRunGruntParser(Reader stream, String source, PigContext context) {
        super(stream);
        this.source = source;
        this.context = context;
    }

    public String getResult() { return sb.toString(); }
    
    public boolean parseStopOnError() throws IOException {
        done = false;
        while (!done) {
            try {
                parse();
            } catch (ParseException e) {
               throw new ParserException("Dry run parsing failed", e);
            }
        }
        return hasMacro;
    }
    
    @Override
    public void prompt() {

    }

    @Override
    protected void quit() {
        done = true;
    }

    @Override
    protected void printAliases() throws IOException {

    }

    @Override
    protected void processFsCommand(String[] cmdTokens) throws IOException {
        String cmds = LoadFunc.join((AbstractList<String>)Arrays.asList(cmdTokens), " ");
        sb.append("fs ").append(cmds).append("\n");
    }

    @Override
    protected void processShCommand(String[] cmdTokens) throws IOException {
        String cmds = LoadFunc.join((AbstractList<String>)Arrays.asList(cmdTokens), " ");
        sb.append("sh ").append(cmds).append("\n");
    }
    
    @Override
    protected void processSQLCommand(String cmd) throws IOException {
        sb.append("sql ").append(cmd).append("\n");
    }

    @Override
    protected void processDescribe(String alias) throws IOException {
        sb.append("describe ").append(alias).append(";\n");
    }

    @Override
    protected void processExplain(String alias, String script,
            boolean isVerbose, String format, String target,
            List<String> params, List<String> files) throws IOException,
            ParseException {
        sb.append("explain ");
        if (script != null) {
            sb.append("-script ").append(script).append(" ");
        }
        if (target != null) {
            sb.append("-out ").append(target).append(" ");
        }
        if (isVerbose) sb.append("-brief ");
        if (format != null && format.equals("dot")) {
            sb.append("-dot ");
        }
        if (params != null) {
            for (String param : params) {
                sb.append("-param ").append(param).append(" ");
            }
        }
        if (files != null) {
            for (String file : files) {
                sb.append("-param_file ").append(file).append(" ");
            }
        }
        if (alias != null) {
            sb.append(alias);
        }
        sb.append("\n");
    }

    @Override
    protected void processRegister(String jar) throws IOException {
        sb.append("register ").append(jar).append(";\n");
    }

    @Override
    protected void processRegister(String path, String scriptingEngine,
            String namespace) throws IOException, ParseException {
        sb.append("register '").append(path).append("'");
        if (scriptingEngine != null) {
            sb.append(" using ").append(scriptingEngine);
        }
        if (namespace != null) {
            sb.append(" as ").append(namespace);
        }
        sb.append(";\n");

    }

    @Override
    protected void processSet(String key, String value) throws IOException,
            ParseException {
        sb.append("set ").append(key).append(" ").append(value).append("\n");
    }

    @Override
    protected void processCat(String path) throws IOException {
        sb.append("cat ").append(path).append("\n");
    }

    @Override
    protected void processCD(String path) throws IOException {
        sb.append("cd ").append(path).append("\n");
    }

    @Override
    protected void processDump(String alias) throws IOException {
        sb.append("dump ").append(alias).append("\n");
    }

    @Override
    protected void processKill(String jobid) throws IOException {
        sb.append("kill ").append(jobid).append("\n");
    }

    @Override
    protected void processLS(String path) throws IOException {
        sb.append("ls");
        if (path != null) sb.append(" ").append(path);
        sb.append("\n");
    }

    @Override
    protected void processPWD() throws IOException {
        sb.append("pwd\n");
    }
    
    @Override
    protected void printHelp() {

    }
    
    @Override
	protected void processHistory(boolean withNumbers) {
		
	}

    @Override
    protected void processMove(String src, String dst) throws IOException {
        sb.append("mv ").append(src).append(" ").append(dst).append("\n");
    }

    @Override
    protected void processCopy(String src, String dst) throws IOException {
        sb.append("cp ").append(src).append(" ").append(dst).append("\n");
    }

    @Override
    protected void processCopyToLocal(String src, String dst)
            throws IOException {
        sb.append("CopyToLocal ").append(src).append(" ").append(dst).append("\n");
    }

    @Override
    protected void processCopyFromLocal(String src, String dst)
            throws IOException {
        sb.append("CopyFrom,Local ").append(src).append(" ").append(dst).append("\n");
    }

    @Override
    protected void processMkdir(String dir) throws IOException {
        sb.append("mkdir ").append(dir).append("\n");
    }

    @Override
    protected void processPig(String cmd) throws IOException {
        
        int start = getLineNumber();
       
        StringBuilder blder = new StringBuilder();
        for (int i = 1; i < start; i++) {
            blder.append("\n");            
        }
        
        if (cmd.charAt(cmd.length() - 1) != ';') {
            cmd += ";";
        }
        
        blder.append(cmd);
        cmd = blder.toString();
        
        CommonTokenStream tokenStream = QueryParserDriver.tokenize(cmd, source);
        Tree ast = null;

        try {
            ast = QueryParserDriver.parse( tokenStream );
        } catch(RuntimeException ex) {
            throw new ParserException( ex.getMessage() );
        }    

        if (!hasMacro) {
            List<CommonTree> importNodes = new ArrayList<CommonTree>();
            List<CommonTree> macroNodes = new ArrayList<CommonTree>();
            List<CommonTree> inlineNodes = new ArrayList<CommonTree>();
            
            QueryParserDriver.traverseImport(ast, importNodes);
            QueryParserDriver.traverse(ast, macroNodes, inlineNodes);
            
            if (!importNodes.isEmpty() || !macroNodes.isEmpty()
                            || !inlineNodes.isEmpty()) {
                hasMacro = true;
            }
        }

        if (parserTree == null) {
            parserTree = ast;
        } else {
            int n = ast.getChildCount();
            for (int i = 0; i < n; i++) {
                parserTree.addChild(ast.getChild(i));
            }
        }
 
        CommonTree dup = (CommonTree)parserTree.dupNode();
        dup.addChildren(((CommonTree)parserTree).getChildren());

        QueryParserDriver driver = new QueryParserDriver(context, "0",
                new HashMap<String, String>());
        Tree newAst = driver.expandMacro(dup);
        
        CommonTreeNodeStream nodes = new CommonTreeNodeStream( newAst );
        AstPrinter walker = new AstPrinter( nodes );
        try {
            walker.query();
        } catch (RecognitionException e) {
            throw new ParserException("Failed to print AST for command " + cmd,
                    e);
        }

        String result = walker.getResult().trim();
       
        if (!result.isEmpty()) {
            String[] lines = result.split("\n");
            for (int i = toSkip; i < lines.length; i++) {
                sb.append(lines[i]).append("\n");
                toSkip++;
            }   
        }
    }

    @Override
    protected void processRemove(String path, String opt) throws IOException {
        if (opt == null || !opt.equalsIgnoreCase("force")) {
            sb.append("rm ");
        } else {
            sb.append("rmf ");
        }
        sb.append(path).append("\n");
    }

    @Override
    protected void processIllustrate(String alias, String script,
            String target, List<String> params, List<String> files)
            throws IOException, ParseException {
        sb.append("illustrate ");
        if (script != null) {
            sb.append("-script ").append(script).append(" ");
        }
        if (target != null) {
            sb.append("-out ").append(target).append(" ");
        }
        if (params != null) {
            for (String param : params) {
                sb.append("-param ").append(param).append(" ");
            }
        }
        if (files != null) {
            for (String file : files) {
                sb.append("-param_file ").append(file).append(" ");
            }
        }
        if (alias != null) {
            sb.append(alias);
        }
        sb.append("\n");
    }

    @Override
    protected void processScript(String script, boolean batch,
            List<String> params, List<String> files) throws IOException,
            ParseException {
        if (batch) sb.append("exec ");
        else sb.append("run ");
        if (params != null) {
            for (String param : params) {
                sb.append("-param ").append(param).append(" ");
            }
        }
        if (files != null) {
            for (String file : files) {
                sb.append("-param_file ").append(file).append(" ");
            }
        }
        sb.append(script).append("\n");
    }

}
