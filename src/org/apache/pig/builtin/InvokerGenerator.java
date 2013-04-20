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
package org.apache.pig.builtin;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.ICONST_2;
import static org.objectweb.asm.Opcodes.ICONST_3;
import static org.objectweb.asm.Opcodes.ICONST_4;
import static org.objectweb.asm.Opcodes.ICONST_5;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_6;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.python.google.common.collect.Maps;

import com.google.common.collect.Sets;

//TODO need to add support for ANY Pig type!
//TODO statically cache the generated code based on the input Strings
public class InvokerGenerator extends EvalFunc<Object> {
    private String className_;
    private String methodName_;
    private String[] argumentTypes_;

    private boolean isInitialized = false;

    private InvokerFunction generatedFunction;
    private Schema outputSchema;

    private static int uniqueId = 0;

    private static final Map<Class<?>, Byte> returnTypeMap = new HashMap<Class<?>, Byte>() {{
       put(String.class, DataType.CHARARRAY);
       put(Integer.class, DataType.INTEGER);
       put(Long.class, DataType.LONG);
       put(Float.class, DataType.FLOAT);
       put(Double.class, DataType.DOUBLE);
       put(Boolean.class, DataType.BOOLEAN);
       //put(byte[].class, DataType.BYTEARRAY);
       put(Integer.TYPE, DataType.INTEGER);
       put(Long.TYPE, DataType.LONG);
       put(Float.TYPE, DataType.FLOAT);
       put(Double.TYPE, DataType.DOUBLE);
       put(Boolean.TYPE, DataType.BOOLEAN);

    }};

    private static final Map<Class<?>, Class<?>> inverseTypeMap = new HashMap<Class<?>, Class<?>>() {{
       put(Integer.class, Integer.TYPE);
       put(Long.class, Long.TYPE);
       put(Float.class, Float.TYPE);
       put(Double.class, Double.TYPE);
       put(Boolean.class, Boolean.TYPE);
       put(Integer.TYPE, Integer.class);
       put(Long.TYPE, Long.class);
       put(Float.TYPE, Float.class);
       put(Double.TYPE, Double.class);
       put(Boolean.TYPE, Boolean.class);
    }};

    private static final Map<Class<?>, String> primitiveSignature = new HashMap<Class<?>, String>() {{
        put(Integer.TYPE, "I");
        put(Long.TYPE, "J");
        put(Float.TYPE, "F");
        put(Double.TYPE, "D");
        put(Boolean.TYPE, "Z");
    }};

    private static final Map<String,Class<?>> nameToClassObjectMap = new HashMap<String,Class<?>>() {{
        put("String",String.class);
        put("Integer", Integer.class);
        put("int", Integer.TYPE);
        put("Long", Long.class);
        put("long", Long.TYPE);
        put("Float", Float.class);
        put("float", Float.TYPE);
        put("Double", Double.class);
        put("double", Double.TYPE);
        put("Boolean", Boolean.class);
        put("boolean", Boolean.TYPE);
        //put("byte[]", byte[].class);
        put("java.lang.String",String.class);
        put("java.lang.Integer", Integer.class);
        put("java.lang.Long", Long.class);
        put("java.lang.Float", Float.class);
        put("java.lang.Double", Double.class);
        put("java.lang.Boolean", Boolean.class);
    }};

    public InvokerGenerator(String className, String methodName, String argumentTypes) {
        className_ = className;
        methodName_ = methodName;
        argumentTypes_ = argumentTypes.split(",");
        if ("".equals(argumentTypes)) {
            argumentTypes_ = new String[0];
        }
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        if (!isInitialized)
            initialize();

        return generatedFunction.eval(input);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (!isInitialized)
            initialize();

        return outputSchema;
    }

    private static int getUniqueId() {
        return uniqueId++;
    }

    protected void initialize() {
        Class<?> clazz;
        try {
            clazz = PigContext.resolveClassName(className_); //TODO I should probably be using this for all of the Class<?> resolution
        } catch (IOException e) {
            throw new RuntimeException("Given className not found: " + className_, e);
        }

        Class<?>[] arguments = getArgumentClassArray(argumentTypes_);

        Method method;
        try {
            method = clazz.getMethod(methodName_, arguments); //must match exactly
        } catch (SecurityException e) {
            throw new RuntimeException("Not allowed to call given method["+methodName_+"] on class ["+className_+"] with arguments: " + Arrays.toString(argumentTypes_), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Given method name ["+methodName_+"] does not exist on class ["+className_+"] with arguments: " + Arrays.toString(argumentTypes_), e);
        }
        boolean isStatic = Modifier.isStatic(method.getModifiers());

        Class<?> returnClazz = method.getReturnType();
        Byte type;
        if (returnClazz.isPrimitive()) {
            type = returnTypeMap.get(inverseTypeMap.get(returnClazz));
        } else {
            type = returnTypeMap.get(returnClazz);
        }

        //TODO add functionality so that if the user pairs this witha  cast that it will let you return object
        if (type == null) {
            throw new RuntimeException("Function returns invalid type: " + returnClazz);
        }

        outputSchema = new Schema();
        outputSchema.add(new Schema.FieldSchema(null, type));

        generatedFunction = generateInvokerFunction("InvokerFunction_"+getUniqueId(), method, isStatic, arguments);

        isInitialized = true;
    }

    private Class<?>[] getArgumentClassArray(String[] argumentTypes) {
        Class<?>[] arguments = new Class<?>[argumentTypes.length];
        for (int i = 0; i < argumentTypes.length; i++) {
            try {
                arguments[i]= nameToClassObjectMap.get(argumentTypes[i]);
                if (arguments[i] == null) {
                    arguments[i] = PigContext.resolveClassName(argumentTypes[i]);
                }
            } catch (IOException e) {
                throw new RuntimeException("Unable to find class in PigContext: " + argumentTypes[i], e);
            }
        }
        return arguments;
    }

    private InvokerFunction generateInvokerFunction(String className, Method method, boolean isStatic, Class<?>[] arguments) {
        byte[] byteCode = generateInvokerFunctionBytecode(className, method, isStatic, arguments);

        return ByteClassLoader.getInvokerFunction(className, byteCode);
    }

    private byte[] generateInvokerFunctionBytecode(String className, Method method, boolean isStatic, Class<?>[] arguments) {
        boolean isInterface = method.getDeclaringClass().isInterface();

        ClassWriter cw = new ClassWriter(0);
        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, className, null, "java/lang/Object", new String[] { "org/apache/pig/builtin/InvokerFunction" });

        makeConstructor(cw);

        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "eval", "(Lorg/apache/pig/data/Tuple;)Ljava/lang/Object;", null, new String[] { "java/io/IOException" });
        mv.visitCode();

        int next = 2;
        //this will get the arguments from the Tuple, cast them, and astore them
        int begin = 0;
        if (!isStatic)
            loadAndStoreArgument(mv, begin++, next++, getMethodStyleName(method.getDeclaringClass()));

        for (int i = 0; i < arguments.length; i++)
            loadAndStoreArgument(mv, i + begin, next++, getMethodStyleName(getObjectVersion(arguments[i])));

        //puts the arguments on the stack
        next = 2;

        if (!isStatic) {
            mv.visitVarInsn(ALOAD, next++); //put the method receiver on the stack
        }

        for (Class<?> arg : arguments) {
            mv.visitVarInsn(ALOAD, next++);
            unboxIfPrimitive(mv, arg);
        }
        String signature = buildSignatureString(arguments, method.getReturnType());
        mv.visitMethodInsn(isStatic ? INVOKESTATIC : isInterface ? INVOKEINTERFACE : INVOKEVIRTUAL, getMethodStyleName(method.getDeclaringClass()), method.getName(), signature);
        boxIfPrimitive(mv, method.getReturnType()); //TODO does this work?
        mv.visitInsn(ARETURN);
        mv.visitMaxs(2, (isStatic ? 2 : 3) + arguments.length);
        mv.visitEnd();

        cw.visitEnd();

        return cw.toByteArray();
    }

    private String buildSignatureString(Class<?>[] arguments, Class<?> returnClazz) {
        String sig = "(";
        for (Class<?> arg : arguments) {
            if (!arg.isPrimitive())
                sig += "L" + getMethodStyleName(arg) + ";";
            else
                sig += getMethodStyleName(arg);
        }
        sig += ")";

        if (!returnClazz.isPrimitive()) {
            sig += "L" + getMethodStyleName(returnClazz) + ";";
        } else {
            sig += getMethodStyleName(returnClazz);
        }
        return sig;

    }

    private Class<?> getObjectVersion(Class<?> clazz) {
        if (clazz.isPrimitive()) {
            return inverseTypeMap.get(clazz);
        }
        return clazz;

    }

    private String getMethodStyleName(Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return clazz.getCanonicalName().replaceAll("\\.","/");
        }
        return primitiveSignature.get(clazz);
    }

    private void boxIfPrimitive(MethodVisitor mv, Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return;
        }
        String boxedClass = getMethodStyleName(inverseTypeMap.get(clazz));
        mv.visitMethodInsn(INVOKESTATIC, boxedClass, "valueOf", "("+getMethodStyleName(clazz)+")L"+boxedClass+";");
    }

    private void unboxIfPrimitive(MethodVisitor mv, Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return;
        }
        String methodName = clazz.getSimpleName()+"Value";
        mv.visitMethodInsn(INVOKEVIRTUAL, getMethodStyleName(inverseTypeMap.get(clazz)), methodName, "()"+getMethodStyleName(clazz));
    }

    private void loadAndStoreArgument(MethodVisitor mv, int loadIdx, int storeIdx, String castName) {
        mv.visitVarInsn(ALOAD, 1); //loads the 1st argument
        addConst(mv, loadIdx);
        mv.visitMethodInsn(INVOKEINTERFACE, "org/apache/pig/data/Tuple", "get", "(I)Ljava/lang/Object;");
        mv.visitTypeInsn(CHECKCAST, castName);
        mv.visitVarInsn(ASTORE, storeIdx);
    }

    private void addConst(MethodVisitor mv, int idx) {
        switch (idx) {
            case(0): mv.visitInsn(ICONST_0); break;
            case(1): mv.visitInsn(ICONST_1); break;
            case(2): mv.visitInsn(ICONST_2); break;
            case(3): mv.visitInsn(ICONST_3); break;
            case(4): mv.visitInsn(ICONST_4); break;
            case(5): mv.visitInsn(ICONST_5); break;
            default:
                throw new RuntimeException("Invalid index given to addConst: " + idx);
        }
    }

    private void makeConstructor(ClassWriter cw) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    static class ByteClassLoader extends ClassLoader {
        private byte[] buf;

        public ByteClassLoader(byte[] buf) {
            this.buf = buf;
        }

        public Class<InvokerFunction> findClass(String name) {
            return (Class<InvokerFunction>)defineClass(name, buf, 0, buf.length);
        }

        public static InvokerFunction getInvokerFunction(String name, byte[] buf) {
            try {
                return new ByteClassLoader(buf).findClass(name).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
