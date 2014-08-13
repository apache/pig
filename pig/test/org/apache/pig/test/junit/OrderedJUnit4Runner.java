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
package org.apache.pig.test.junit;

import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

public class OrderedJUnit4Runner extends BlockJUnit4ClassRunner {

    @Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @Target(TYPE)
    public @interface TestOrder {
        String[] value();
    }

    private String testClassName;
    private String[] orderedTestMethods;
    private List<FrameworkMethod> methodsCustomOrder;

    public OrderedJUnit4Runner(Class<?> klass) throws InitializationError {
        super(klass);
        TestOrder testOrder = klass.getAnnotation(TestOrder.class);
        if (testOrder != null) {
            orderedTestMethods = testOrder.value();
        }
        this.testClassName = klass.getName();
    }

    @Override
    protected List<FrameworkMethod> computeTestMethods() {
        List<FrameworkMethod> methodsReflectionOrder = super
                .computeTestMethods();
        if (orderedTestMethods == null || orderedTestMethods.length == 0) {
            return methodsReflectionOrder;
        } else {

            if (methodsCustomOrder == null) {
                // Check for duplicate test methods
                Set<String> uniqueMethods = new HashSet<String>();
                Collections.addAll(uniqueMethods, orderedTestMethods);
                if (uniqueMethods.size() != orderedTestMethods.length) {
                    throw new IllegalArgumentException(
                            "The TestOrder annotation in " + testClassName
                                    + " has duplicate test method names");
                }

                methodsCustomOrder = new ArrayList<FrameworkMethod>();
                for (String method : orderedTestMethods) {
                    for (FrameworkMethod fMethod : methodsReflectionOrder) {
                        if (fMethod.getName().equals(method)) {
                            methodsCustomOrder.add(fMethod);
                            break;
                        }
                    }
                }

                // Check for missing test methods
                methodsReflectionOrder.removeAll(methodsCustomOrder);
                if (methodsReflectionOrder.size() > 0) {
                    String tests = "";
                    for (FrameworkMethod fMethod : methodsReflectionOrder) {
                        tests = tests + fMethod.getName();
                    }
                    throw new IllegalArgumentException(
                            "The TestOrder annotation in " + testClassName
                            + " does not include the following tests which have @Test: "
                            + tests);
                }
            }
            return methodsCustomOrder;
        }
    }

}
