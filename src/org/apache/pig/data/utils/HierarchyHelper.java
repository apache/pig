package org.apache.pig.data.utils;

import java.lang.reflect.Method;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.data.SchemaTuple;

import com.google.common.collect.Sets;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HierarchyHelper {
    private static final Log LOG = LogFactory.getLog(HierarchyHelper.class);

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
     * Important note: currently, even if an intermediate class overrides the function,
     * the ultimate class must still override it if the annotation is present.
     */
    //TODO consider the case of T1, T2 extends T2, and T3 extends T2
    //TODO if T1 has a MustOverride that T2 overrides without its own flag, then should it be ok?
    //TODO currently it will not be ok
    public static boolean verifyMustOverride(Class<?> classToVerify) {
        for (Class<?> clazz : getSuperclasses(classToVerify)) {
            if (!verifyMustOverrideSpecific(classToVerify, clazz)) {
                return false;
            }
        }
        return true;
    }

    private static boolean verifyMustOverrideSpecific(Class<?> classToVerify, Class<?> classWithAnnotations) {
        Set<Method> methodsThatMustBeOverriden = Sets.newHashSet();

        for (Method m : classWithAnnotations.getDeclaredMethods()) {
            if (m.getAnnotation(MustOverride.class) != null) {
                methodsThatMustBeOverriden.add(m);
            }
        }

        outer: for (Method m1 : classToVerify.getDeclaredMethods()) {
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
                LOG.warn("Missing method in class " + classToVerify + ": " + m);
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
