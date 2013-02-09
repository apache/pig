/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.pig.comparator;

public final class ByteExpr extends FixedLengthPrimitive {
  public ByteExpr(int index) {
    super(index, 1);
  }

  protected void convertValue(byte[] b, Object o) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Expecting length==1");
    }
    byte l = (Byte) o;
    l ^= 1 << (Byte.SIZE - 1);
    b[0] = l;
  }

  @Override
  protected String getType() {
    return "Byte";
  }
}
