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
package org.apache.pig.data;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Queue;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

/**
 * This class encapsulates the generation of SchemaTuples, as well as some logic
 * around shipping code to the distributed cache.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SchemaTupleClassGenerator {
    private static final Log LOG = LogFactory.getLog(SchemaTupleClassGenerator.class);

    private SchemaTupleClassGenerator() {}

    public static enum GenContext {
        UDF ("pig.schematuple.udf", true, GenerateUdf.class),
        LOAD ("pig.schematuple.load", true, GenerateLoad.class),
        JOIN ("pig.schematuple.join", true, GenerateJoin.class),
        FORCE_LOAD ("pig.schematuple.force", true, GenerateForceLoad.class);

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.TYPE)
        public @interface GenerateUdf {}

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.TYPE)
        public @interface GenerateLoad {}

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.TYPE)
        public @interface GenerateJoin {}

        @Retention(RetentionPolicy.RUNTIME)
        @Target(ElementType.TYPE)
        public @interface GenerateForceLoad {}

        private String key;
        private boolean defaultValue;
        private Class<?> annotation;

        GenContext(String key, boolean defaultValue, Class<?> annotation) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.annotation = annotation;
        }

        public String key() {
            return key;
        }

        public String getAnnotationCanonicalName() {
            return annotation.getCanonicalName();
        }

        @SuppressWarnings("unchecked")
        public boolean shouldGenerate(Class clazz) {
            return clazz.getAnnotation(annotation) != null;
        }

        public boolean shouldGenerate(Configuration conf) {
            String shouldString = conf.get(key);
            if (shouldString == null) {
                return defaultValue;
            }
            return Boolean.parseBoolean(shouldString);
        }
    }

    /**
     * This value is used to distinguish all of the generated code.
     * The general naming scheme used is SchemaTupe_identifier. Note that
     * identifiers are incremented before code is actually generated.
     */
    private static int nextGlobalClassIdentifier = 0;

    /**
     * This class actually generates the code for a given Schema.
     * @param   schema
     * @param   true or false depending on whether it should be appendable
     * @param   identifier
     * @param   a list of contexts in which the SchemaTuple is intended to be instantiated
     */
    protected static void generateSchemaTuple(Schema s, boolean appendable, int id, File codeDir, GenContext... contexts) {
        StringBuilder contextAnnotations = new StringBuilder();
        for (GenContext context : contexts) {
            LOG.info("Including context: " + context);
            contextAnnotations.append("@").append(context.getAnnotationCanonicalName()).append("\n");
        }

        String codeString = produceCodeString(s, appendable, id, contextAnnotations.toString(), codeDir);

        String name = "SchemaTuple_" + id;

        LOG.info("Compiling class " + name + " for Schema: " + s + ", and appendability: " + appendable);

        compileCodeString(name, codeString, codeDir);
    }

    //Should it inherit the
    private static int generateSchemaTuple(Schema s, boolean appendable, File codeDir, GenContext... contexts) {
        int id = SchemaTupleClassGenerator.getNextGlobalClassIdentifier();

        generateSchemaTuple(s, appendable, id, codeDir, contexts);

        return id;
    }

    /**
     * This method generates the actual SchemaTuple for the given Schema.
     * @param   schema
     * @param   whether the class should be appendable
     * @param   identifier
     * @return  the generated class's implementation
     */
    private static String produceCodeString(Schema s, boolean appendable, int id, String contextAnnotations, File codeDir) {
        TypeInFunctionStringOutFactory f = new TypeInFunctionStringOutFactory(s, id, appendable, contextAnnotations, codeDir);

        for (Schema.FieldSchema fs : s.getFields()) {
            f.process(fs);
        }

        return f.end();
    }

    protected static int getNextGlobalClassIdentifier() {
        return nextGlobalClassIdentifier++;
    }

    /**
     * This method takes generated code, and compiles it down to a class file. It will output
     * the generated class file to the static temporary directory for generated code. Note
     * that the compiler will use the classpath that Pig is instantiated with, as well as the
     * generated directory.
     *
     * @param String of generated code
     * @param name of class
     */
    //TODO in the future, we can use ASM to generate the bytecode directly.
    private static void compileCodeString(String className, String generatedCodeString, File codeDir) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = Lists.newArrayList(new JavaSourceFromString(className, generatedCodeString));

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

        String tempDir = codeDir.getAbsolutePath();

        String classPath = System.getProperty("java.class.path") + ":" + tempDir;
        LOG.debug("Compiling SchemaTuple code with classpath: " + classPath);

        List<String> optionList = Lists.newArrayList();
        // Adds the current classpath to the compiler along with our generated code
        optionList.add("-classpath");
        optionList.add(classPath);
        optionList.add("-d");
        optionList.add(tempDir);

        if (!compiler.getTask(null, fileManager, diagnostics, optionList, null, compilationUnits).call()) {
            LOG.warn("Error compiling: " + className + ". Printing compilation errors and shutting down.");
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                LOG.warn("Error on line " + diagnostic.getLineNumber() + ": " + diagnostic.getMessage(Locale.US));
            }
            throw new RuntimeException("Unable to compile code string:\n" + generatedCodeString);
        }

        LOG.info("Successfully compiled class: " + className);
    }

    /**
     * This class allows code to be generated directly from a String, instead of having to be
     * on disk.
     */
    private static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.','/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    static class CompareToSpecificString extends TypeInFunctionStringOut {
        private int id;

        public CompareToSpecificString(int id, boolean appendable) {
            super(appendable);
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("protected int generatedCodeCompareToSpecific(SchemaTuple_"+id+" t) {");
            add("    int i = 0;");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    i = compare(checkIfNull_" + fieldNum + "(), getPos_"
                    + fieldNum + "(), t.checkIfNull_" + fieldNum + "(), t.getPos_"
                    + fieldNum + "());");
            add("    if (i != 0) {");
            add("        return i;");
            add("    }");
        }

        public void end() {
            add("    return i;");
            add("}");
        }
    }

    //TODO clear up how it deals with nulls etc. IE is the logic correct
    static class CompareToString extends TypeInFunctionStringOut {
        private int id;

        public CompareToString(int id) {
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("protected int generatedCodeCompareTo(SchemaTuple t, boolean checkType) {");
            add("    int i;");
        }

        boolean compTup = false;
        boolean compStr = false;
        boolean compIsNull = false;
        boolean compByte = false;

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("        i = compareWithElementAtPos(checkIfNull_" + fieldNum + "(), getPos_" + fieldNum + "(), t, " + fieldNum + ");");
            add("        if (i != 0) {");
            add("            return i;");
            add("        }");
        }

        public void end() {
            add("    return 0;");
            add("}");
        }
    }

    static class HashCode extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public int generatedCodeHashCode() {");
            add("    int h = 17;");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    h = hashCodePiece(h, getPos_" + fieldPos + "(), checkIfNull_" + fieldPos + "());");
        }

        public void end() {
            add("    return h;");
            add("}");
        }
    }

    static class FieldString extends TypeInFunctionStringOut {
        private List<Queue<Integer>> listOfQueuesForIds;
        private Schema schema;

        private int primitives = 0;
        private int isNulls = 0;

        private int booleanBytes = 0;
        private int booleans = 0;
        private File codeDir;

        public void prepare() {
            String s = schema.toString();
            s = s.substring(1, s.length() - 1);
            add("private static Schema schema = staticSchemaGen(\"" + s + "\");");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                if (isPrimitive() && (primitives++ % 8 == 0)) {
                    add("private byte isNull_"+ isNulls++ +" = (byte)0xFF;");
                }

                if (isBoolean()) {
                    if (booleans++ % 8 == 0) {
                        add("private byte booleanByte_"+ booleanBytes++ +";");
                    }
                } else {
                    add("private "+typeName()+" pos_"+fieldPos+";");
                }
            } else {
                int id = SchemaTupleClassGenerator.generateSchemaTuple(fs.schema, isAppendable(), codeDir());

                for (Queue<Integer> q : listOfQueuesForIds) {
                    q.add(id);
                }

                add("private SchemaTuple_"+id+" pos_"+fieldPos+";");
            }
        }

        @Override
        public void end() {
            addBreak();
            add("@Override");
            add("public Schema getSchema() {");
            add("    return schema;");
            add("}");
            addBreak();
        }

        public FieldString(File codeDir, List<Queue<Integer>> listOfQueuesForIds, Schema schema, boolean appendable) {
            super(appendable);
            this.codeDir = codeDir;
            this.listOfQueuesForIds = listOfQueuesForIds;
            this.schema = schema;
        }

        public File codeDir() {
            return codeDir;
        }
    }

    static class SetPosString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;

        private int byteField = 0; //this is for setting booleans
        private int byteIncr = 0; //this is for counting the booleans we've encountered

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                add("public void setPos_"+fieldPos+"("+typeName()+" v) {");
                if (isPrimitive())
                    add("    setNull_"+fieldPos+"(false);");

                if (!isBoolean()) {
                    add("    pos_"+fieldPos+" = v;");
                } else {
                    add("    booleanByte_" + byteField + " = BytesHelper.setBitByPos(booleanByte_" + byteField + ", v, " + byteIncr++ + ");");

                    if (byteIncr % 8 == 0) {
                        byteIncr = 0;
                        byteField++;
                    }
                }

                add("}");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("public void setPos_"+fieldPos+"(SchemaTuple_"+nestedSchemaTupleId+" t) {");
                add("    pos_" + fieldPos + " = t;");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(SchemaTuple t) {");
                add("    if (pos_"+fieldPos+" == null) {");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    }");
                add("    pos_" + fieldPos + ".setAndCatch(t);");
                add("}");
                addBreak();
                add("public void setPos_"+fieldPos+"(Tuple t) {");
                add("    if (pos_"+fieldPos+" == null) {");
                add("        pos_"+fieldPos+" = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("    }");
                add("    pos_" + fieldPos + ".setAndCatch(t);");
                add("}");
            }
            addBreak();
        }

        public SetPosString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class ListSetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public void generatedCodeSetIterator(Iterator<Object> it) throws ExecException {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    setPos_"+fieldPos+"(unbox(it.next(), getDummy_"+fieldPos+"()));");
        }

        public void end() {
            add("}");
        }
    }

    static class GenericSetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public void generatedCodeSetField(int fieldNum, Object val) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"):");
            add("        if (val == null) {");
            add("            setNull_" + fieldPos + "(true);");
            add("            return;");
            add("        }");
            add("        setPos_"+fieldPos+"(unbox(val, getDummy_"+fieldPos+"()));");
            add("        break;");
        }

        public void end() {
            add("    default:");
            add("        throw new ExecException(\"Invalid index given to set: \" + fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class GenericGetString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public Object generatedCodeGetField(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"): return checkIfNull_"+fieldPos+"() ? null : box(getPos_"+fieldPos+"());");
        }

        public void end() {
            add("    default: throw new ExecException(\"Invalid index given to get: \" + fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class GeneralIsNullString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public boolean isGeneratedCodeFieldNull(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    case ("+fieldPos+"): return checkIfNull_"+fieldPos+"();");
        }

        public void end() {
            add("    default: throw new ExecException(\"Invalid index given: \" + fieldNum);");
            add("    }");
            add("}");
        }
    }

    static class CheckIfNullString extends TypeInFunctionStringOut {
        private int nullByte = 0; //the byte_ val
        private int byteIncr = 0; //the mask we're on

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("public boolean checkIfNull_" + fieldPos + "() {");
            if (isPrimitive()) {
                add("    return BytesHelper.getBitByPos(isNull_" + nullByte + ", " + byteIncr++ +");");
                if (byteIncr % 8 == 0) {
                    byteIncr = 0;
                    nullByte++;
                }
            } else {
               add("    return pos_" + fieldPos + " == null;");
            }
            add("}");
            addBreak();
        }
    }

   static class SetNullString extends TypeInFunctionStringOut {
        private int nullByte = 0; //the byte_ val
        private int byteIncr = 0; //the mask we're on

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("public void setNull_"+fieldPos+"(boolean b) {");
            if (isPrimitive()) {
                add("    isNull_" + nullByte + " = BytesHelper.setBitByPos(isNull_" + nullByte + ", b, " + byteIncr++ + ");");
                if (byteIncr % 8 == 0) {
                    byteIncr = 0;
                    nullByte++;
                }
            } else {
                add("    if (b) {");
                add("        pos_" + fieldPos + " = null;");
                add("    }");
            }
            add("}");
            addBreak();
        }
    }

    //TODO should this do something different if t is null?
    static class SetEqualToSchemaTupleSpecificString extends TypeInFunctionStringOut {
        private int id;

        public void prepare() {
            add("@Override");
            add("protected SchemaTuple generatedCodeSetSpecific(SchemaTuple_"+id+" t) {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("    if (t.checkIfNull_" + fieldPos + "()) {");
            add("        setNull_" + fieldPos + "(true);");
            add("    } else {");
            add("        setPos_"+fieldPos+"(t.getPos_"+fieldPos+"());");
            add("    }");
            addBreak();
        }

        public void end() {
            add("    return this;");
            add("}");
            addBreak();
        }

        public SetEqualToSchemaTupleSpecificString(int id) {
            this.id = id;
        }
    }

    //this has to write the null state of all the fields, not just the null bytes, though those
    //will have to be reconstructed
    static class WriteNullsString extends TypeInFunctionStringOut {
        String s = "    boolean[] b = {\n";

        public void prepare() {
            add("@Override");
            add("protected boolean[] generatedCodeNullsArray() throws IOException {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            s += "        checkIfNull_"+fieldPos+"(),\n";
        }

        public void end() {
            s = s.substring(0, s.length() - 2) + "\n    };";
            add(s);
            add("    return b;");
            add("}");
            addBreak();
        }

        public WriteNullsString(boolean appendable) {
            super(appendable);
        }
    }

   static class ReadString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;
        int ct = 0;

        private int booleans = 0;
        private int booleanBytes = 0;

        public void prepare() {
            add("@Override");
            add("protected void generatedCodeReadFields(DataInput in, boolean[] b) throws IOException {");
        }

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (isBoolean()) {
                if (booleans++ % 8 == 0) {
                    booleanBytes++;
                }
            } else if (!isTuple()) {
                add("    if (b["+fieldPos+"]) {");
                add("        setNull_"+fieldPos+"(true);");
                add("    } else {");
                add("        setPos_"+fieldPos+"(read(in, pos_"+fieldPos+"));");
                add("    }");
                addBreak();
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("    if (b["+fieldPos+"]) {");
                add("        setNull_"+fieldPos+"(true);");
                add("    } else {");
                add("        SchemaTuple_"+nestedSchemaTupleId+" st = new SchemaTuple_"+nestedSchemaTupleId+"();");
                add("        st.readFields(in);");
                add("        setPos_"+fieldPos+"(st);");
                add("    }");
                addBreak();
            }
            ct++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++) {
                add("    booleanByte_"+i+" = in.readByte();");
            }
            add("}");
            addBreak();
        }

        public ReadString(Queue<Integer> idQueue, boolean appendable) {
            super(appendable);
            this.idQueue = idQueue;
        }
    }


    static class WriteString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("protected void generatedCodeWriteElements(DataOutput out) throws IOException {");
        }

        private int booleans = 0;
        private int booleanBytes = 0;

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isBoolean()) {
                add("    if (!checkIfNull_"+fieldPos+"()) {");
                add("        write(out, pos_"+fieldPos+");");
                add("    }");
                addBreak();
            }

            if (isBoolean() && booleans++ % 8 == 0)
                booleanBytes++;
        }

        public void end() {
            for (int i = 0; i < booleanBytes; i++) {
                add("    out.writeByte(booleanByte_"+i+");");
            }
            add("}");
            addBreak();
        }
    }

    //TODO need to include all of the objects from Schema (have it implement it's own getMemorySize()?
    static class MemorySizeString extends TypeInFunctionStringOut {
        private int size = 0;

        String s = "    return SizeUtil.roundToEight(";

        public void prepare() {
            add("@Override");
            add("public long getGeneratedCodeMemorySize() {");
        }

        private int booleans = 0;
        private int primitives = 0;

        //TODO a null array or object variable still takes up space for the pointer, yes?
        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (isInt() || isFloat()) {
                size += 4;
            } else if (isLong() || isDouble()) {
                size += 8;
            } else if (isBytearray()) {
                s += "(pos_"+fieldPos+" == null ? 8 : SizeUtil.roundToEight(12 + pos_"+fieldPos+".length) * 8) + ";
            } else if (isString()) {
                s += "(pos_"+fieldPos+" == null ? 8 : SizeUtil.getPigObjMemSize(pos_"+fieldPos+")) + ";
            } else if (isBoolean()) {
                if (booleans++ % 8 == 0) {
                    size++; //accounts for the byte used to store boolean values
                }
            } else {
                s += "(pos_"+fieldPos+" == null ? 8 : pos_"+fieldPos+".getMemorySize()) + ";
            }

            if (isPrimitive() && primitives++ % 8 == 0) {
                size++; //accounts for the null byte
            }
        }

        public void end() {
            s += size + ");";
            add(s);
            add("}");
            addBreak();
        }
    }

    static class GetDummyString extends TypeInFunctionStringOut {
        public void process(int fieldPos, Schema.FieldSchema fs) {
            add("public "+typeName()+" getDummy_"+fieldPos+"() {");
            switch (fs.type) {
            case (DataType.INTEGER): add("    return 0;"); break;
            case (DataType.LONG): add("    return 0L;"); break;
            case (DataType.FLOAT): add("    return 0.0f;"); break;
            case (DataType.DOUBLE): add("    return 0.0;"); break;
            case (DataType.BOOLEAN): add("    return true;"); break;
            case (DataType.BYTEARRAY): add("    return (byte[])null;"); break;
            case (DataType.CHARARRAY): add("    return (String)null;"); break;
            case (DataType.TUPLE): add("    return (Tuple)null;"); break;
            }
            add("}");
            addBreak();
        }
    }

    static class GetPosString extends TypeInFunctionStringOut {
        private Queue<Integer> idQueue;

        private int booleanByte = 0;
        private int booleans;

        public void process(int fieldPos, Schema.FieldSchema fs) {
            if (!isTuple()) {
                add("public "+typeName()+" getPos_"+fieldPos+"() {");
            } else {
                int nestedSchemaTupleId = idQueue.remove();
                add("public SchemaTuple_" + nestedSchemaTupleId + " getPos_"+fieldPos+"() {");
            }
            if (isBoolean()) {
                add("    return BytesHelper.getBitByPos(booleanByte_" + booleanByte + ", " + booleans++ + ");");
                if (booleans % 8 == 0) {
                    booleanByte++;
                    booleans = 0;
                }
            } else {
                add("    return pos_"+fieldPos+";");
            }
            add("}");
            addBreak();
        }

        public GetPosString(Queue<Integer> idQueue) {
            this.idQueue = idQueue;
        }
    }

    static class GetSchemaTupleIdentifierString extends TypeInFunctionStringOut {
        private int id;

        public void end() {
            add("@Override");
            add("public int getSchemaTupleIdentifier() {");
            add("    return "+id+";");
            add("}");
            addBreak();
        }

        public GetSchemaTupleIdentifierString(int id) {
            this.id = id;
        }
    }

    static class SchemaSizeString extends TypeInFunctionStringOut {
        int i = 0;

        public void process(int fieldNum, Schema.FieldSchema fS) {
            i++;
        }

        public void end() {
            add("@Override");
            add("protected int schemaSize() {");
            add("    return " + i + ";");
            add("}");
            addBreak();
        }
    }

    static class SizeString extends TypeInFunctionStringOut {
        int i = 0;

        public void process(int fieldNum, Schema.FieldSchema fS) {
            i++;
        }

        public void end() {
            add("@Override");
            add("protected int generatedCodeSize() {");
            add("    return " + i + ";");
            add("}");
            addBreak();
        }

        public SizeString(boolean appendable) {
            super(appendable);
        }
    }

    static class GetTypeString extends TypeInFunctionStringOut {
        public void prepare() {
            add("@Override");
            add("public byte getGeneratedCodeFieldType(int fieldNum) throws ExecException {");
            add("    switch (fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    case ("+fieldNum+"): return "+fs.type+";");
        }

        public void end() {
            add("    default: throw new ExecException(\"Invalid index given: \" + fieldNum);");
            add("    }");
            add("}");
            addBreak();
        }
    }

    static class SetEqualToSchemaTupleString extends TypeInFunctionStringOut {
        int id;

        public SetEqualToSchemaTupleString(int id) {
            this.id = id;
        }

        public void prepare() {
            add("@Override");
            add("protected SchemaTuple generatedCodeSet(SchemaTuple t, boolean checkClass) throws ExecException {");
            add("    if (checkClass && t instanceof SchemaTuple_"+id+") {");
            add("        return setSpecific((SchemaTuple_"+id+")t);");
            add("    }");
            addBreak();
            add("    if (t.size() < schemaSize()) {");
            add("        throw new ExecException(\"Given SchemaTuple does not have as many fields as \"+getClass()+\" (\"+t.size()+\" vs \"+schemaSize()+\")\");");
            add("    }");
            addBreak();
            add("    List<Schema.FieldSchema> theirFS = t.getSchema().getFields();");
            addBreak();
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            add("    if ("+fs.type+" != theirFS.get("+fieldNum+").type) {");
            add("        throw new ExecException(\"Given SchemaTuple does not match current in field " + fieldNum + ". Expected type: " + fs.type + ", found: \" + theirFS.get("+fieldNum+").type);");
            add("    }");
            add("    if (t.isNull("+fieldNum+")) {");
            add("        setNull_"+fieldNum+"(true);");
            add("    } else {");
            if (!isTuple()) {
                add("        setPos_"+fieldNum+"(t.get" + proper(fs.type) + "("+fieldNum+"));");
            } else {
                add("        setPos_"+fieldNum+"((Tuple)t.get("+fieldNum+"));");
            }
            add("    }");
            addBreak();
        }

        public void end() {
            add("    return this;");
            add("}");
        }
    }

   static class TypeAwareGetString extends TypeAwareSetString {
        public TypeAwareGetString(byte type) {
            super(type);
        }

        public void prepare() {
            add("@Override");
            add("protected "+name()+" generatedCodeGet"+properName()+"(int fieldNum) throws ExecException {");
            add("    switch(fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (fs.type==thisType()) {
                add("    case ("+fieldNum+"): return returnUnlessNull(checkIfNull_"+fieldNum+"(), getPos_"+fieldNum+"());");
            }
        }

        public void end() {
            add("    default:");
            add("        return unbox"+properName()+"(getTypeAwareBase(fieldNum, \""+name()+"\"));");
            add("    }");
            add("}");
        }
    }

    static class TypeAwareSetString extends TypeInFunctionStringOut {
        private byte type;

        public TypeAwareSetString(byte type) {
            this.type = type;
        }

        public byte thisType() {
            return type;
        }

        public String name() {
            return typeName(type);
        }

        public String properName() {
            return proper(thisType());
        }

        public void prepare() {
            add("@Override");
            add("protected void generatedCodeSet"+properName()+"(int fieldNum, "+name()+" val) throws ExecException {");
            add("    switch(fieldNum) {");
        }

        public void process(int fieldNum, Schema.FieldSchema fs) {
            if (fs.type==thisType())
                add("    case ("+fieldNum+"): setPos_"+fieldNum+"(val); break;");
        }

        public void end() {
            add("    default: setTypeAwareBase(fieldNum, val, \""+name()+"\");");
            add("    }");
            add("}");
        }
    }

    //TODO need to use StringBuilder for all concatenation, not +
    static class TypeInFunctionStringOutFactory {
        private List<TypeInFunctionStringOut> listOfFutureMethods = Lists.newArrayList();
        private int id;
        private boolean appendable;
        private String contextAnnotations;

        public TypeInFunctionStringOutFactory(Schema s, int id, boolean appendable, String contextAnnotations, File codeDir) {
            this.id = id;
            this.appendable = appendable;
            this.contextAnnotations = contextAnnotations;

            Queue<Integer> nextNestedSchemaIdForSetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForGetPos = Lists.newLinkedList();
            Queue<Integer> nextNestedSchemaIdForReadField = Lists.newLinkedList();

            List<Queue<Integer>> listOfQueuesForIds = Lists.newArrayList(nextNestedSchemaIdForSetPos, nextNestedSchemaIdForGetPos, nextNestedSchemaIdForReadField);

            listOfFutureMethods.add(new FieldString(codeDir, listOfQueuesForIds, s, appendable)); //has to be run first
            listOfFutureMethods.add(new SetPosString(nextNestedSchemaIdForSetPos));
            listOfFutureMethods.add(new GetPosString(nextNestedSchemaIdForGetPos));
            listOfFutureMethods.add(new GetDummyString());
            listOfFutureMethods.add(new GenericSetString());
            listOfFutureMethods.add(new GenericGetString());
            listOfFutureMethods.add(new GeneralIsNullString());
            listOfFutureMethods.add(new CheckIfNullString());
            listOfFutureMethods.add(new SetNullString());
            listOfFutureMethods.add(new SetEqualToSchemaTupleSpecificString(id));
            listOfFutureMethods.add(new WriteNullsString(appendable));
            listOfFutureMethods.add(new ReadString(nextNestedSchemaIdForReadField, appendable));
            listOfFutureMethods.add(new WriteString());
            listOfFutureMethods.add(new SizeString(appendable));
            listOfFutureMethods.add(new MemorySizeString());
            listOfFutureMethods.add(new GetSchemaTupleIdentifierString(id));
            listOfFutureMethods.add(new HashCode());
            listOfFutureMethods.add(new SchemaSizeString());
            listOfFutureMethods.add(new GetTypeString());
            listOfFutureMethods.add(new CompareToString(id));
            listOfFutureMethods.add(new CompareToSpecificString(id, appendable));
            listOfFutureMethods.add(new SetEqualToSchemaTupleString(id));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.INTEGER));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.LONG));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.FLOAT));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.DOUBLE));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.BYTEARRAY));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.CHARARRAY));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.BOOLEAN));
            listOfFutureMethods.add(new TypeAwareSetString(DataType.TUPLE));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.INTEGER));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.LONG));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.FLOAT));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.DOUBLE));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.BYTEARRAY));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.CHARARRAY));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.BOOLEAN));
            listOfFutureMethods.add(new TypeAwareGetString(DataType.TUPLE));
            listOfFutureMethods.add(new ListSetString());

            for (TypeInFunctionStringOut t : listOfFutureMethods) {
                t.prepare();
            }
        }

        public void process(Schema.FieldSchema fs) {
            for (TypeInFunctionStringOut t : listOfFutureMethods)
                t.prepareProcess(fs);
        }

        public String end() {
            StringBuilder head =
                new StringBuilder()
                    .append("import java.util.List;\n")
                    .append("import java.util.Iterator;\n")
                    .append("import java.io.DataOutput;\n")
                    .append("import java.io.DataInput;\n")
                    .append("import java.io.IOException;\n")
                    .append("\n")
                    .append("import com.google.common.collect.Lists;\n")
                    .append("\n")
                    .append("import org.apache.pig.data.DataType;\n")
                    .append("import org.apache.pig.data.Tuple;\n")
                    .append("import org.apache.pig.data.SchemaTuple;\n")
                    .append("import org.apache.pig.data.AppendableSchemaTuple;\n")
                    .append("import org.apache.pig.data.utils.SedesHelper;\n")
                    .append("import org.apache.pig.data.utils.BytesHelper;\n")
                    .append("import org.apache.pig.data.DataByteArray;\n")
                    .append("import org.apache.pig.data.BinInterSedes;\n")
                    .append("import org.apache.pig.impl.util.Utils;\n")
                    .append("import org.apache.pig.impl.logicalLayer.schema.Schema;\n")
                    .append("import org.apache.pig.impl.logicalLayer.FrontendException;\n")
                    .append("import org.apache.pig.backend.executionengine.ExecException;\n")
                    .append("import org.apache.pig.data.SizeUtil;\n")
                    .append("import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;\n")
                    .append("\n")
                    .append(contextAnnotations);

            if (appendable) {
                head.append("public class SchemaTuple_"+id+" extends AppendableSchemaTuple<SchemaTuple_"+id+"> {\n");
            } else {
                head.append("public class SchemaTuple_"+id+" extends SchemaTuple<SchemaTuple_"+id+"> {\n");
            }

            for (TypeInFunctionStringOut t : listOfFutureMethods) {
                t.end();
                head.append(t.getContent());
            }

            head.append("\n")
                .append("    @Override\n")
                .append("    public SchemaTupleQuickGenerator<SchemaTuple_" + id + "> getQuickGenerator() {\n")
                .append("        return new SchemaTupleQuickGenerator<SchemaTuple_" + id + ">() {\n")
                .append("            @Override\n")
                .append("            public SchemaTuple_" + id + " make() {\n")
                .append("                return new SchemaTuple_" + id + "();\n")
                .append("            }\n")
                .append("        };\n")
                .append("    }\n");

            return head.append("}").toString();
        }
    }

    static class TypeInFunctionStringOut {
        private int fieldPos = 0;
        private StringBuilder content = new StringBuilder();
        private byte type;

        public void prepare() {}
        public void process(int fieldPos, Schema.FieldSchema fs) {}
        public void end() {}

        public int appendable = -1;

        public StringBuilder getContent() {
            return content;
        }

        public TypeInFunctionStringOut() {
            add("// this code generated by " + getClass());
            addBreak();
        }

        public boolean isAppendable() {
            if (appendable == -1) {
                throw new RuntimeException("Need to be given appendable status in " + getClass());
            }
            return appendable == 1;
        }

        public TypeInFunctionStringOut(boolean appendable) {
            this();
            this.appendable = appendable ? 1 : 0;
        }

        public StringBuilder spaces(int indent) {
            StringBuilder out = new StringBuilder();
            String space = "    ";
            for (int i = 0; i < indent; i++) {
                out.append(space);
            }
            return out;
        }

        public void add(String s) {
            for (String str : s.split("\\n")) {
                content.append(spaces(1).append(str).append("\n"));
            }
        }

        public void addBreak() {
            content.append("\n");
        }

        public void prepareProcess(Schema.FieldSchema fs) {
            type = fs.type;

            if (type==DataType.MAP || type==DataType.BAG)
                throw new RuntimeException("Map and Bag currently not supported by SchemaTuple");

            process(fieldPos, fs);
            fieldPos++;
        }

        public boolean isInt() {
            return type == DataType.INTEGER;
        }

        public boolean isLong() {
            return type == DataType.LONG;
        }

        public boolean isFloat() {
            return type == DataType.FLOAT;
        }

        public boolean isDouble() {
            return type == DataType.DOUBLE;
        }

        public boolean isPrimitive() {
            return isInt() || isLong() || isFloat() || isDouble() || isBoolean();
        }

        public boolean isBoolean() {
            return type == DataType.BOOLEAN;
        }

        public boolean isString() {
            return type == DataType.CHARARRAY;
        }

        public boolean isBytearray() {
            return type == DataType.BYTEARRAY;
        }

        public boolean isTuple() {
            return type == DataType.TUPLE;
        }

        public boolean isObject() {
            return !isPrimitive();
        }

        public String typeName() {
            return typeName(type);
        }

        public String typeName(byte type) {
            switch(type) {
                case (DataType.INTEGER): return "int";
                case (DataType.LONG): return "long";
                case (DataType.FLOAT): return "float";
                case (DataType.DOUBLE): return "double";
                case (DataType.BYTEARRAY): return "byte[]";
                case (DataType.CHARARRAY): return "String";
                case (DataType.BOOLEAN): return "boolean";
                case (DataType.TUPLE): return "Tuple";
                default: throw new RuntimeException("Can't return String for given type " + DataType.findTypeName(type));
            }
        }

        public String proper(byte type) {
            String s = typeName(type);
            return type == DataType.BYTEARRAY ? "Bytes" : s.substring(0,1).toUpperCase() + s.substring(1);
        }
    }
}