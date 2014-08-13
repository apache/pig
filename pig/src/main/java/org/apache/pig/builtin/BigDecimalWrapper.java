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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * Max and min seeds cannot be defined to BigDecimal as the value could go as large as
 * The computer allows. This wrapper is used to provide seed for MIN and MAX functions
 * in AlgrebraicBigDecimalMathBase.java
 */

public class BigDecimalWrapper extends BigDecimal {
    private static final long serialVersionUID = 1L;

    private enum BigDecimalInfinity {
        NEGATIVE_INFINITY,
        POSITIVE_INFINITY
    }

    private BigDecimalInfinity infinity;

    // BigDecimal constructors initialized to keep compiler happy
    public BigDecimalWrapper(BigInteger val) {
        super(val);
    }

    public BigDecimalWrapper(BigInteger unscaledVal, int scale) {
        super(unscaledVal, scale);
    }

    public BigDecimalWrapper(BigInteger unscaledVal, int scale, MathContext mc) {
        super(unscaledVal, scale, mc);
    }

    public BigDecimalWrapper(BigInteger val, MathContext mc) {
        super(val, mc);
    }

    public BigDecimalWrapper(char[] in) {
        super(in);
    }

    public BigDecimalWrapper(char[] in, int offset, int len) {
        super(in, offset, len);

    }

    public BigDecimalWrapper(char[] in, int offset, int len, MathContext mc) {
        super(in, offset, len, mc);
    }

    public BigDecimalWrapper(char[] in, MathContext mc) {
        super(in, mc);
    }

    public BigDecimalWrapper(double val) {
        super(val);
    }

    public BigDecimalWrapper(double val, MathContext mc) {
        super(val, mc);
    }

    public BigDecimalWrapper(int val) {
        super(val);
    }

    public BigDecimalWrapper(int val, MathContext mc) {
        super(val,mc);
    }

    public BigDecimalWrapper(long val) {
        super(val);
    }

    public BigDecimalWrapper(long val, MathContext mc) {
        super(val, mc);
    }

    public BigDecimalWrapper(String val) {
        super(val);
    }

    public BigDecimalWrapper(String val, MathContext mc) {
        super(val, mc);
    }

    private BigDecimalWrapper(BigDecimalInfinity in) {
        super(0);
        infinity = in;
    }

    public boolean isPositiveInfinity() {
        return (infinity==BigDecimalInfinity.POSITIVE_INFINITY);
    }

    public boolean isNegativeInfinity() {
        return (infinity==BigDecimalInfinity.NEGATIVE_INFINITY);
    }

    static public BigDecimalWrapper NEGATIVE_INFINITY() {
        return new BigDecimalWrapper(BigDecimalInfinity.NEGATIVE_INFINITY);
    }

    static public BigDecimalWrapper POSITIVE_INFINITY() {
        return new BigDecimalWrapper(BigDecimalInfinity.POSITIVE_INFINITY);
    }
}
