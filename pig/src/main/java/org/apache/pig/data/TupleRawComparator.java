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
package org.apache.pig.data;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.RawComparator;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This interface is intended to compare Tuples. The semantics of Tuple comparison must take into account null values in
 * different ways. According to SQL semantics nulls are not equal. But for other Pig/Latin statements nulls must be
 * grouped together. This interface allows to check if there are null fields in the tuples compared using this
 * comparator. This method is meaningful only when the tuples are determined to be equal by the
 * {@link #compare(byte[],int,int,byte[],int,int)} method.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("rawtypes")
public interface TupleRawComparator extends RawComparator, Configurable {
    /**
     * Checks if one of the compared tuples had a null field. This method is meaningful only when
     * {@link #compare(byte[],int,int,byte[],int,int)} has returned a zero value (i.e. tuples are determined to be
     * equal).
     * 
     * @return true if one of the compared tuples had a null field, false otherwise.
     */
    public boolean hasComparedTupleNull();
}
