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
            if (HierarchyHelper.methodsEqual(m, clazzMethod)) {
                return clazzMethod.getAnnotation(NotImplemented.class) != null;
            }
        }
        return isNotImplementedAnnotationPresent(m, clazz.getSuperclass());
    }

    public static RuntimeException methodNotImplemented() {
        StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        StackTraceElement pre = ste[ste.length - 2];
        return new RuntimeException(pre.getMethodName() + " not implemented in " + pre.getClassName());
    }
}
