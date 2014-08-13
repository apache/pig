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

package org.apache.pig.impl.util.avro;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.avro.util.Utf8;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.base.Function;

/**
 * Wrapper for map objects, so we can translate UTF8 objects to
 * Strings if we encounter them.
 */
public final class AvroMapWrapper implements Map<CharSequence, Object> {

  /**
   * The map contained in the wrapper object.
   */
  private Map<CharSequence, Object> innerMap;
  private boolean isUtf8key;

  /**
   * Creates a new AvroMapWrapper object from the map object {@m}.
   * @param m The map to wrap.
   */
  public AvroMapWrapper(final Map<CharSequence, Object> m) {
    innerMap = m;
    if (m.keySet().size() > 0 && m.keySet().iterator().next() instanceof Utf8)
      isUtf8key = true;
    else
      isUtf8key = false;
  }

  @Override
  public int size() {
    return innerMap.size();
  }

  @Override
  public boolean isEmpty() {
    return innerMap.isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return innerMap.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return innerMap.containsValue(value);
  }

  @Override
  public Object get(final Object key) {
    Object v = null;
    if (isUtf8key && !(key instanceof Utf8)) {
      v = innerMap.get(new Utf8((String) key));
    } else {
      v = innerMap.get(key);
    }

    if (v instanceof Utf8) {
      return v.toString();
    } else {
      return v;
    }
  }

  @Override
  public Object put(final CharSequence key, final Object value) {
    return innerMap.put(key, value);
  }

  @Override
  public Object remove(final Object key) {
    return innerMap.remove(key);
  }

  @Override
  public void putAll(
      final Map<? extends CharSequence, ? extends Object> m) {
    innerMap.putAll(m);
  }

  @Override
  public void clear() {
    innerMap.clear();
  }

  @Override
  public Set<CharSequence> keySet() {
    return innerMap.keySet();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Collection<Object> values() {
    return Collections2.transform(innerMap.values(),
        new Function() {
            @Override
            public Object apply(final Object v) {
              if (v instanceof Utf8) {
                return v.toString();
              } else {
                return v;
              }
            }
          }
        );
  }

  @Override
  public Set<java.util.Map.Entry<CharSequence, Object>> entrySet() {
    Set<java.util.Map.Entry<CharSequence, Object>> theSet =
        Sets.newHashSetWithExpectedSize(innerMap.size());
    for (java.util.Map.Entry<CharSequence, Object> e : innerMap.entrySet()) {
      CharSequence k = e.getKey();
      Object v = e.getValue();
      if (k instanceof Utf8) {
        k = k.toString();
      }
      if (v instanceof Utf8) {
        v = v.toString();
      }
      theSet.add(new AbstractMap.SimpleEntry<CharSequence, Object>(k, v));
    }

    return theSet;

  }

}
