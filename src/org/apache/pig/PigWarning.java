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
package org.apache.pig;

/**
 * An enum to enumerate the warning types in Pig
 * 
 */
public enum PigWarning {
    ACCESSING_NON_EXISTENT_FIELD,
    DID_NOT_FIND_LOAD_ONLY_MAP_PLAN,
    DIVIDE_BY_ZERO,
    FIELD_DISCARDED_TYPE_CONVERSION_FAILED,
    GROUP_BY_INCOMPATIBLE_TYPES,
    IMPLICIT_CAST_TO_BAG,
    IMPLICIT_CAST_TO_CHARARRAY,
    IMPLICIT_CAST_TO_DOUBLE,
    IMPLICIT_CAST_TO_FLOAT,
    IMPLICIT_CAST_TO_INT,
    IMPLICIT_CAST_TO_LONG,
    IMPLICIT_CAST_TO_BOOLEAN,
    IMPLICIT_CAST_TO_DATETIME,
    IMPLICIT_CAST_TO_MAP,
    IMPLICIT_CAST_TO_TUPLE,
    TOO_LARGE_FOR_INT,
    MULTI_LEAF_MAP,
    MULTI_LEAF_REDUCE,
    NON_PACKAGE_REDUCE_PLAN_ROOT,
    NON_EMPTY_COMBINE_PLAN,
    PROGRESS_REPORTER_NOT_PROVIDED,
    REDUCE_PLAN_NOT_EMPTY_WHILE_MAP_PLAN_UNDER_PROCESS,
    UDF_WARNING_1, //placeholder for UDF warnings
    UDF_WARNING_2, //placeholder for UDF warnings
    UDF_WARNING_3, //placeholder for UDF warnings
    UDF_WARNING_4, //placeholder for UDF warnings
    UDF_WARNING_5, //placeholder for UDF warnings
    UDF_WARNING_6, //placeholder for UDF warnings
    UDF_WARNING_7, //placeholder for UDF warnings
    UDF_WARNING_8, //placeholder for UDF warnings
    UDF_WARNING_9, //placeholder for UDF warnings
    UDF_WARNING_10, //placeholder for UDF warnings
    UDF_WARNING_11,	//placeholder for UDF warnings
    UDF_WARNING_12,	//placeholder for UDF warnings
    UNABLE_TO_CREATE_FILE_TO_SPILL,
    UNABLE_TO_SPILL,
    UNABLE_TO_CLOSE_SPILL_FILE,
    UNREACHABLE_CODE_BOTH_MAP_AND_REDUCE_PLANS_PROCESSED,
    USING_OVERLOADED_FUNCTION,
    REDUCER_COUNT_LOW,
    NULL_COUNTER_COUNT,
    DELETE_FAILED,
    PROJECTION_INVALID_RANGE
    ;
}
