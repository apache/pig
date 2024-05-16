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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.util.Iterator;

abstract class CachedIteratorTransform<IN, OUT> extends IteratorTransform<IN,OUT> {

    public CachedIteratorTransform(Iterator<IN> delegate) {
        super(delegate);
    }

    private OUT cachedObject = null;

    // in case transform returns a valid null, adding one more flag
    // to determine if the result is cached or not
    private boolean isCached = false;

    private boolean endReached = false;

    // If transform traverses the "delegate" iterator in certain
    // condition (like in
    // SkewedJoinConverter.ToValueFunction.Tuple2TransformIterable)
    // there is no easy way to determine if this iterator has next
    // item or not except to actually call the transform method.
    @Override
    public boolean hasNext() {
        if( endReached ) {
            return false;
        }
        if( !isCached ) {
            try {
                cachedObject = transform(delegate.next());
                isCached = true;
            } catch (java.util.NoSuchElementException ex) {
                cachedObject = null;
                isCached = false;
                endReached = true;
                return false;
            }
        }
        return true;
    }

    @Override
    public OUT next() {
        if( !isCached ) {
            return transform(delegate.next());
        } else {
            OUT retObject = cachedObject;
            cachedObject = null;
            isCached = false;
            return retObject;
        }
    }
}
