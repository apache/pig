package org.apache.pig.data.utils;

import java.lang.reflect.Method;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.HashSet;
import java.util.Stack;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.data.SchemaTuple;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

import com.google.common.collect.Sets;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HierarchyHelper {
    /**
     * This is an annotation used to ensure that the generated
     * code properly implements a number of methods. A number
     * of methods have partial implementations which can be
     * leveraged by the generated code, but must be overriden
     * and called via super.method() to be meaningful.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface MustOverride {}

    private static List<Class<?>> getSuperclasses(Class<?> clazz) {
        List<Class<?>> list = Lists.newLinkedList();
        Class<?> curClass = clazz.getSuperclass();
        while (!curClass.equals(Object.class)) {
            list.add(curClass);
            curClass = curClass.getSuperclass();
        }
        return list;
    }

    /**
     * This code verifies that any MustOverride tags in any parent class are fufilled.
     * Note: in the case that an intermediate class in the class hierarchy overrides
     * the class, that will satisfy the MustOverride. If you want to ensure that the ultimate
     * class also overrides, then any intermediate implementations must be tagged as @MustOverride.
     * Returns true if all MustOverrides are satisfied.
     */
    public static boolean verifyMustOverride(Class<?> classToVerify) {
        Stack<Class<?>> classHierarchy = new Stack<Class<?>>();
        classHierarchy.push(classToVerify);
        Class<?> curClass = classToVerify.getSuperclass();
        while (!curClass.equals(Object.class)) {
            classHierarchy.push(curClass);
            curClass = curClass.getSuperclass();
        }

        Set<MethodKey> methodsNeedingToBeOverriden = new HashSet<MethodKey>();

        while (!classHierarchy.empty()) {
            for (Method m : classHierarchy.pop().getDeclaredMethods()) {
                MethodKey mk = new MethodKey(m);
                if (m.getAnnotation(MustOverride.class) != null) {
                    methodsNeedingToBeOverriden.add(mk);
                } else {
                    methodsNeedingToBeOverriden.remove(mk);
                }
            }
        }

        System.out.println("PRINTING METHODS IN SET"); //remove
        for (MethodKey mk : methodsNeedingToBeOverriden) { //remove
            System.out.println(mk); //remove
        } //remove
        return methodsNeedingToBeOverriden.size() == 0;
    }

    private static class MethodKey {
        private Method m;

        public MethodKey(Method m) {
            this.m = m;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MethodKey)) {
                return false;
            }
            return methodsEqual(m, ((MethodKey)o).get());
        }

        public Method get() {
            return m;
        }

        @Override
        public int hashCode() {
            int c = 17;
            c += m.getName().hashCode();
            c *= 31;
            c += m.getReturnType().hashCode();
            c *= 31;
            for (Object o : m.getParameterTypes()) {
                c += o.hashCode();
                c *= 31;
            }
            return c;
        }

        @Override
        public String toString() {
            return m.toString();
        }
    }

    /**
     * This implements a stripped down version of method equality.
     * method.equals(method) checks to see whether the declaring classes
     * are equal, which we do not want. Instead, we just want to know
     * if the methods are equal assuming that they come from the same
     * class hierarchy (ie generated code which extends SchemaTuple).
     */
    public static boolean methodsEqual(Method m1, Method m2) {
        if (!m1.getName().equals(m2.getName())) {
            return false;
        }

        if (!m1.getReturnType().equals(m2.getReturnType())) {
            return false;
        }

        /* Avoid unnecessary cloning */
        Class[] params1 = m1.getParameterTypes();
        Class[] params2 = m2.getParameterTypes();
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
