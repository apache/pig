/**
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

package org.apache.pig.impl.util;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import com.google.common.collect.Lists;

import org.apache.hadoop.util.Shell;
import org.apache.log4j.Logger;

public class JavaCompilerHelper {
    private static final Logger LOG = Logger.getLogger(JavaCompilerHelper.class);
    
    /**
     * This class allows code to be generated directly from a String, instead of having to be
     * on disk.
     */
    public static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

        public JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.','/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private String classPath;

    public JavaCompilerHelper() {
        this.classPath = System.getProperty("java.class.path");
    }

    public void compile(String target, JavaSourceFromString... sources) {
        LOG.info("compiling java classes to "+target);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

        List<String> optionList = Lists.newArrayList();
        // Adds the current classpath to the compiler along with our generated code
        optionList.add("-classpath");
        optionList.add(classPath);
        optionList.add("-d");
        optionList.add(target);

        if (!compiler.getTask(null, fileManager, diagnostics, optionList, null, Arrays.asList(sources)).call()) {
            LOG.warn("Error compiling Printing compilation errors and shutting down.");
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                LOG.warn("Error on line " + diagnostic.getLineNumber() + ": " + diagnostic.getMessage(Locale.US));
            }
            throw new RuntimeException("Unable to compile");
        }
    }

    public void addToClassPath(String path) {
        this.classPath = this.classPath + System.getProperty("path.separator") + path;
    }

    public String getClassPath() {
        return classPath;
    }
}
