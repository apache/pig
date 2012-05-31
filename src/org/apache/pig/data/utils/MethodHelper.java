package org.apache.pig.data.utils;

import java.lang.reflect.Method;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MethodHelper {
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
}
