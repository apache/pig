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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This interface defines how a loader can support predicate pushdown.
 * If a given loader implements this interface, pig will pushdown predicates based on
 * type of operations supported by the loader on given set of fields.
 * @since Pig 0.14
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface LoadPredicatePushdown {
    /**
     * Find what fields of the data can support predicate pushdown.
     * @param location Location as returned by
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     *
     * @param job The {@link Job} object - this should be used only to obtain
     * cluster properties through {@link Job#getConfiguration()} and not to set/query
     * any runtime job information.
     *
     * @return list of field names that can be pushed down. Implementations
     * should return null to indicate that there are no fields that support predicate pushdown
     *
     * @throws IOException if an exception occurs while retrieving predicate fields
     */
    List<String> getPredicateFields(String location, Job job)
            throws IOException;

    /**
     * Indicate operations on fields supported by the loader for predicate pushdown
     *
     * @return List of operations supported by the predicate pushdown loader
     */
    List<Expression.OpType> getSupportedExpressionTypes();

    /**
     * Push down expression to the loader
     *
     * @param predicate expression to be filtered by the loader.
     * @throws IOException
     */
    void setPushdownPredicate(Expression predicate) throws IOException;

}

