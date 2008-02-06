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

import java.io.DataOutput;
import java.io.IOException;

/**
 * A marker class for a basic data unit.
 */
public abstract class Datum implements Comparable {
    public static final byte ATOM   = 0x50;
    public static final byte BAG    = 0x51;
    public static final byte TUPLE  = 0x60;
    public static final byte MAP  = 0x52;
    public static final byte RECORD_1 = 0x21;
    public static final byte RECORD_2 = 0x31;
    public static final byte RECORD_3 = 0x41;

    public static final int OBJECT_SIZE = 8;
    public static final int REF_SIZE = 4;

    @Override
    public abstract boolean equals(Object o);
    
    public abstract void write(DataOutput out) throws IOException;

    public abstract long getMemorySize();
    
         
}
