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

    /**
     * Counter names used by pig.udf.profile
     */
    public static final String TIME_UDFS_INVOCATION_COUNTER = "approx_invocations";
    public static final String TIME_UDFS_ELAPSED_TIME_COUNTER = "approx_microsecs";

    public static final String TASK_INDEX = "mapreduce.task.index";

    /**
     * This parameter is used to check if the rollup is optimizable or not after going
     * through the RollupHIIOptimizer
     */
    public static final String PIG_HII_ROLLUP_OPTIMIZABLE = "pig.hii.rollup.optimizable";

    /**
     * This parameter stores the value of the pivot position. If the rollup is not optimizable
     * this value will be -1; If the rollup is optimizable: if the user did specify the pivot
     * in the rollup clause, this parameter will get that value; if the user did not specify
     * the pivot in the rollup clause, this parameter will get the value of the median position
     * of the fields in the rollup clause
     */
    public static final String PIG_HII_ROLLUP_PIVOT = "pig.hii.rollup.pivot";

    /**
     * This parameter stores the index of the first field involves in the rollup (or the first field
     * involves in the rollup after changing the position of rollup to the end in case of having cube)
     */
    public static final String PIG_HII_ROLLUP_FIELD_INDEX = "pig.hii.rollup.field.index";

    /**
     * This parameter stores the index of the first field involves in the rollup before
     * changing the position of rollup to the end in case of having cube
     */
    public static final String PIG_HII_ROLLUP_OLD_FIELD_INDEX = "pig.hii.rollup.old.field.index";

    /**
     * This parameter stores the size of total fields which involve in the CUBE clause. For example, we
     * have two CUBE clause:
     * B = CUBE A BY CUBE(year, month, day), ROLLUP(hour, minute, second);
     * B = CUBE A BY ROLLUP(year, month, day, hour, minute, second);
     * So this parameter will be 6 at both cases.
     */
    public static final String PIG_HII_NUMBER_TOTAL_FIELD = "pig.hii.number.total.field";

    /**
     * This parameter stores the number of algebraic functions that used after rollup.
     */
    public static final String PIG_HII_NUMBER_ALGEBRAIC = "pig.hii.number.algebraic";
}