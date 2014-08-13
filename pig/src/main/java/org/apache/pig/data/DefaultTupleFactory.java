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

/**
 * This class was being used to create new tuple instances in some
 * udfs and other places in pig code, even though the tuplefactory creation
 * function (getInstance()) is static in TupleFactory
 *  (ie DefaultTuple could not override it)
 *  A typical call is - DefaultTupleFactory.getInstance().newTuple(..);
 *  
 *  So that such external udfs don't break, a DefaultTupleFactory is present.
 *  Don't use this in your code, use TupleFactory directly instead.
 */

/**
 * @deprecated Use {@link TupleFactory}
 */
@Deprecated 
public class DefaultTupleFactory extends BinSedesTupleFactory {
    @Override
    public Class<? extends TupleRawComparator> tupleRawComparatorClass() {
        return DefaultTuple.getComparatorClass();
    }
}
