package org.apache.pig.data.utils;

import java.lang.reflect.Method;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.Iterator;

import org.apache.pig.data.SchemaTuple;

import com.google.common.collect.Sets;

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

    /**
     * This code verifies that every method in SchemaTuple annoted with MustOverride
     * is overriden in the given Class.
     */
    public static boolean verifyMustOverride(Class<? extends SchemaTuple> clazz) {
        Set<Method> methodsThatMustBeOverriden = Sets.newHashSet();

        for (Method m : SchemaTuple.class.getDeclaredMethods()) {
            if (m.getAnnotation(MustOverride.class) != null) {
                methodsThatMustBeOverriden.add(m);
            }
        }

        outer: for (Method m1 : clazz.getDeclaredMethods()) {
            Iterator<Method> it = methodsThatMustBeOverriden.iterator();
            while (it.hasNext()) {
                if (methodsEqual(m1, it.next())) {
                    it.remove();
                    continue outer;
                }
            }
        }

        if (methodsThatMustBeOverriden.size() > 0) {
            for (Method m : methodsThatMustBeOverriden) {
                System.err.println("Missing method in class " + clazz + ": " + m);
            }
            return false;
        } else {
            return true;
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
