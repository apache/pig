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
package org.apache.pig;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
  * This class is used to manage JVM Reuse in case of execution engines like Tez
 * and Spark.
 *
 * Static data members of a UDF, LoadFunc or StoreFunc class
 * need to be reset or object references made null or reinitialized
 * when the jvm container is reused for new tasks.
 *
 * Example usage to perform static data cleanup in a UDF as follows.
 *
 * public class MyUDF extends EvalFunc<Tuple> {
 *   private static int numInvocations = 0;
 *   private static Reporter reporter;
 *
 *   static {
 *      // Register this class for static data cleanup
 *      JVMReuseManager.getInstance().registerForStaticDataCleanup(MyUDF.class);
 *   }
 *
 *   // Write a public static method that performs the cleanup
 *   // and annotate it with @StaticDataCleanup
 *   @StaticDataCleanup
 *   public static void staticDataCleanup() {
 *      numInvocations = 0;
 *      reporter = null;
 *   }
 *   #### UDF Code goes here ######
 * }
 *
 * @since Pig 0.14
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JVMReuseManager {

    private static Log LOG = LogFactory.getLog(JVMReuseImpl.class);
    private List<Method> cleanupMethods = new ArrayList<Method>();
    private static JVMReuseManager instance = new JVMReuseManager();

    private JVMReuseManager() {
    }

    public static JVMReuseManager getInstance() {
        return instance;
    }

    public void registerForStaticDataCleanup(Class<?> clazz) {
        for (Method method : clazz.getMethods()) {
            if (method.isAnnotationPresent(StaticDataCleanup.class)) {
                if (!(Modifier.isStatic(method.getModifiers())
                        && Modifier.isPublic(method.getModifiers()))) {
                    throw new RuntimeException(
                            "Method " + method.getName() + " in class " + clazz.getName() +
                             "should be public and static as it is annotated with  ");
                }
                LOG.debug("Method " + method.getName() + " in class "
                        + method.getDeclaringClass()
                        + " registered for static data cleanup");
                instance.cleanupMethods.add(method);
            }
        }
    }

    List<Method> getStaticDataCleanupMethods() {
        return cleanupMethods;
    }

}
