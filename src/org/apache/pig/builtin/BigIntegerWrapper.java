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
package org.apache.pig.builtin;

import java.math.BigInteger;
import java.util.Random;

/**
 * Max and min seeds cannot be defined to BigInteger as the value could go as large as
 * The computer allows. This wrapper is used to provide seed for MIN and MAX functions
 * in AlgrebraicBigIntegerMathBase.java
 */

public class BigIntegerWrapper extends BigInteger {
    private static final long serialVersionUID = 1L;

    private enum BigIntegerInfinity{
        NEGATIVE_INFINITY,
        POSITIVE_INFINITY
    }

    private BigIntegerInfinity infinity;

    // BigInteger constructors initialized to keep compiler happy
    public BigIntegerWrapper(byte[] val) {
        super(val);
    }

    public BigIntegerWrapper(int signum, byte[] magnitude) {
        super(signum, magnitude);
    }

    public BigIntegerWrapper(int bitLength, int certainty, Random rnd) {
        super(bitLength, certainty, rnd);
    }

    public BigIntegerWrapper(int numBits, Random rnd) {
        super(numBits, rnd);
    }

    public BigIntegerWrapper(String val) {
        super(val);
    }

    public BigIntegerWrapper(String val, int radix) {
        super(val, radix);
    }

    private BigIntegerWrapper(BigIntegerInfinity in) {
        super("0");
        infinity = in;
    }

    public boolean isPositiveInfinity() {
        return (infinity==BigIntegerInfinity.POSITIVE_INFINITY);
    }

    public boolean isNegativeInfinity() {
        return (infinity==BigIntegerInfinity.NEGATIVE_INFINITY);
    }

    static public BigIntegerWrapper NEGATIVE_INFINITY() {
        return new BigIntegerWrapper(BigIntegerInfinity.NEGATIVE_INFINITY);
    }

    static public BigIntegerWrapper POSITIVE_INFINITY() {
        return new BigIntegerWrapper(BigIntegerInfinity.POSITIVE_INFINITY);
    }
}
