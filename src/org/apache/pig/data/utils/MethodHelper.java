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
package org.apache.pig.data.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MethodHelper {
    private MethodHelper() {
    }

    /**
     * This is an annotation which allows a class to signal that while it is "implementing"
     * a method because it is specified by a parent class or interface, that the implementation
     * just throws an exception, because it is not implemented.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface NotImplemented {}

    /**
     * Given a method and a class, this will return true if the method is declared in the class,
     * and if it is, if the NotImplemented annotation is present. This method will recurse through
     * the parent class hierarchy until it finds the first instance of the method at hand, and then it will
     * return accordingly.
     */
    public static boolean isNotImplementedAnnotationPresent(Method m, Class<?> clazz) {
        if (clazz.equals(Object.class)) {
            return false;
        }
        for (Method clazzMethod : clazz.getDeclaredMethods()) {
            if (MethodHelper.methodSignatureEqual(m, clazzMethod)) {
                return clazzMethod.getAnnotation(NotImplemented.class) != null;
            }
        }
        return isNotImplementedAnnotationPresent(m, clazz.getSuperclass());
    }

    public static RuntimeException methodNotImplemented() {
        StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        StackTraceElement pre = ste[ste.length - 2];
        return new UnsupportedOperationException(pre.getMethodName() + " not implemented in " + pre.getClassName());
    }

    /**
     * This implements a stripped down version of method equality.
     * method.equals(method) checks to see whether the declaring classes
     * are equal, which we do not want. Instead, we just want to know
     * if the methods are equal assuming that they come from the same
     * class hierarchy (ie generated code which extends SchemaTuple).
     */
    public static boolean methodSignatureEqual(Method m1, Method m2) {
        if (!m1.getName().equals(m2.getName())) {
            return false;
        }
    
        if (!m1.getReturnType().equals(m2.getReturnType())) {
            return false;
        }
    
        /* Avoid unnecessary cloning */
        Class<?>[] params1 = m1.getParameterTypes();
        Class<?>[] params2 = m2.getParameterTypes();
        if (params1.length == params2.length) {
            for (int i = 0; i < params1.length; i++) {
                if (!params1[i].equals(params2[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
