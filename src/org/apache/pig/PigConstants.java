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

import org.apache.pig.classification.InterfaceAudience;

@InterfaceAudience.Public
public class PigConstants {
    private PigConstants() {
        throw new IllegalStateException();
    }

    /**
     * This key is used in the job conf to let the various jobs know what code was
     * generated.
     */
    public static final String GENERATED_CLASSES_KEY = "pig.schematuple.classes";

    /**
     * This key is used when a job is run in local mode to pass the location of the generated code
     * from the front-end to the "back-end" (which, in this case, is in the same JVM).
     */
    public static final String LOCAL_CODE_DIR = "pig.schematuple.local.dir";

    // This makes it easy to turn SchemaTuple on globally.
    public static final boolean SCHEMA_TUPLE_ON_BY_DEFAULT = false;

    /**
     * {@value} is a comma-separated list of optimizer rules to disable;
     * by default, all rules are enabled.
     */
    public static final String PIG_OPTIMIZER_RULES_DISABLED_KEY = "pig.optimizer.rules.disabled";

    /**
     * Prefix used by pig to configure local mode configuration
     */
    public static final String PIG_LOCAL_CONF_PREFIX = "pig.local.";

}