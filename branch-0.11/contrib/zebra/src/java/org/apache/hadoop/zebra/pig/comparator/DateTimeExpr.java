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

import org.joda.time.DateTime;


public final class DateTimeExpr extends FixedLengthPrimitive {
  private static final int ONE_MINUTE = 60000;
  public DateTimeExpr(int index) {
    super(index, (Long.SIZE + Short.SIZE) / Byte.SIZE);
  }

  protected void convertValue(byte[] b, Object o) {
    if (b.length != (Long.SIZE + Short.SIZE) / Byte.SIZE) {
      throw new IllegalArgumentException("Incorrect buffer size");
    }
    long dt = ((DateTime) o).getMillis();
    dt ^= 1L << (Long.SIZE - 1);
    b[7] = (byte) dt;
    b[6] = (byte) (dt >> 8);
    b[5] = (byte) (dt >> 16);
    b[4] = (byte) (dt >> 24);
    b[3] = (byte) (dt >> 32);
    b[2] = (byte) (dt >> 40);
    b[1] = (byte) (dt >> 48);
    b[0] = (byte) (dt >> 56);
    
    short dtz = (short) (((DateTime) o).getZone().getOffset((DateTime) o) / ONE_MINUTE);
    dtz ^= 1 << (Short.SIZE - 1);
    b[9] = (byte) dtz;
    b[8] = (byte) (dtz >> 8);
  }

  @Override
  protected String getType() {
    return "DateTime";
  }
}
