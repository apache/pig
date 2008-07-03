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

package org.apache.pig.piggybank.test.evaluation;

import org.apache.pig.piggybank.evaluation.math.ABS;
import org.apache.pig.piggybank.evaluation.math.ACOS;
import org.apache.pig.piggybank.evaluation.math.ASIN;
import org.apache.pig.piggybank.evaluation.math.ATAN;
import org.apache.pig.piggybank.evaluation.math.ATAN2;
import org.apache.pig.piggybank.evaluation.math.CBRT;
import org.apache.pig.piggybank.evaluation.math.CEIL;
import org.apache.pig.piggybank.evaluation.math.COS;
import org.apache.pig.piggybank.evaluation.math.COSH;
import org.apache.pig.piggybank.evaluation.math.EXP;
import org.apache.pig.piggybank.evaluation.math.EXPM1;
import org.apache.pig.piggybank.evaluation.math.FLOOR;
import org.apache.pig.piggybank.evaluation.math.HYPOT;
import org.apache.pig.piggybank.evaluation.math.IEEEremainder;
import org.apache.pig.piggybank.evaluation.math.LOG;
import org.apache.pig.piggybank.evaluation.math.LOG10;
import org.apache.pig.piggybank.evaluation.math.LOG1P;
import org.apache.pig.piggybank.evaluation.math.MAX;
import org.apache.pig.piggybank.evaluation.math.MIN;
import org.apache.pig.piggybank.evaluation.math.POW;
import org.apache.pig.piggybank.evaluation.math.RINT;
import org.apache.pig.piggybank.evaluation.math.ROUND;
import org.apache.pig.piggybank.evaluation.math.SCALB;
import org.apache.pig.piggybank.evaluation.math.SIGNUM;
import org.apache.pig.piggybank.evaluation.math.SIN;
import org.apache.pig.piggybank.evaluation.math.SINH;
import org.apache.pig.piggybank.evaluation.math.SQRT;
import org.apache.pig.piggybank.evaluation.math.TAN;
import org.apache.pig.piggybank.evaluation.math.TANH;
import org.apache.pig.piggybank.evaluation.math.ULP;
import org.apache.pig.piggybank.evaluation.math.copySign;
import org.apache.pig.piggybank.evaluation.math.getExponent;
import org.apache.pig.piggybank.evaluation.math.nextAfter;
import org.apache.pig.piggybank.evaluation.math.toDegrees;
import org.apache.pig.piggybank.evaluation.math.toRadians;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;


import junit.framework.TestCase;

public class TestMathUDF extends TestCase{
    public double delta = 0.001;
    
	 public void testABS() throws Exception {
	        EvalFunc<DataAtom> ABS = new ABS();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, -1.0);
	        DataAtom output = new DataAtom();
	        ABS.exec(tup, output);
	        double expected = 1.0;
	        double actual = (new Double(output.strval())).doubleValue();
            assertEquals(actual, expected, delta);
	    }
	 
	 
	 public void testACOS() throws Exception {
	        EvalFunc<DataAtom> ACOS = new ACOS();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        ACOS.exec(tup, output);
	        double expected = Math.acos(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 
	 public void testASIN() throws Exception {
	        EvalFunc<DataAtom> ASIN = new ASIN();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        ASIN.exec(tup, output);
	        double expected = Math.asin(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testATAN() throws Exception {
	        EvalFunc<DataAtom> ATAN = new ATAN();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        ATAN.exec(tup, output);
	        double expected = Math.atan(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testATAN2() throws Exception {
	        EvalFunc<DataAtom> ATAN2 = new ATAN2();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, 0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        ATAN2.exec(tup, output);
	        double expected = Math.atan2(0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testCBRT() throws Exception{
		 	EvalFunc<DataAtom> CBRT = new CBRT();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        CBRT.exec(tup, output);
	        double expected = Math.cbrt(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testCEIL() throws Exception{
		 	EvalFunc<DataAtom> CEIL = new CEIL();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        CEIL.exec(tup, output);
	        double expected = Math.ceil(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testcopySign() throws Exception {
	        EvalFunc<DataAtom> copySign = new copySign();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        copySign.exec(tup, output);
	        double expected = Math.copySign(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testCOS() throws Exception{
		 	EvalFunc<DataAtom> COS = new COS();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        COS.exec(tup, output);
	        double expected = Math.cos(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testCOSH() throws Exception{
		 	EvalFunc<DataAtom> COSH = new COSH();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        COSH.exec(tup, output);
	        double expected = Math.cosh(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testEXP() throws Exception{
		 	EvalFunc<DataAtom> EXP = new EXP();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        EXP.exec(tup, output);
	        double expected = Math.exp(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testEXPM1() throws Exception{
		 	EvalFunc<DataAtom> EXPM1 = new EXPM1();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        EXPM1.exec(tup, output);
	        double expected = Math.expm1(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testFLOOR() throws Exception{
		 	EvalFunc<DataAtom> FLOOR = new FLOOR();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        FLOOR.exec(tup, output);
	        double expected = Math.floor(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testgetExponent() throws Exception{
		 	EvalFunc<DataAtom> getExponent = new getExponent();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, -0.5);
	        DataAtom output = new DataAtom();
	        getExponent.exec(tup, output);
	        double expected = Math.getExponent(-0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testHYPOT() throws Exception {
	        EvalFunc<DataAtom> HYPOT = new HYPOT();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        HYPOT.exec(tup, output);
	        double expected = Math.hypot(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testIEEEremainder() throws Exception {
	        EvalFunc<DataAtom> IEEEremainder = new IEEEremainder();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        IEEEremainder.exec(tup, output);
	        double expected = Math.IEEEremainder(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testLOG() throws Exception{
		 	EvalFunc<DataAtom> LOG = new LOG();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        LOG.exec(tup, output);
	        double expected = Math.log(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testLOG10() throws Exception{
		 	EvalFunc<DataAtom> LOG10 = new LOG10();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        LOG10.exec(tup, output);
	        double expected = Math.log10(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testLOG1P() throws Exception{
		 	EvalFunc<DataAtom> LOG1P = new LOG1P();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        LOG1P.exec(tup, output);
	        double expected = Math.log1p(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testMAX() throws Exception {
	        EvalFunc<DataAtom> MAX = new MAX();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        MAX.exec(tup, output);
	        double expected = Math.max(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testMIN() throws Exception {
	        EvalFunc<DataAtom> MIN = new MIN();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        MIN.exec(tup, output);
	        double expected = Math.min(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testnextAfter() throws Exception {
	        EvalFunc<DataAtom> nextAfter = new nextAfter();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, -0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        nextAfter.exec(tup, output);
	        double expected = Math.nextAfter(-0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 
	 public void testPOW() throws Exception {
	        EvalFunc<DataAtom> POW = new POW();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, 0.5);
	        tup.setField(1,0.6);
	        DataAtom output = new DataAtom();
	        POW.exec(tup, output);
	        double expected = Math.pow(0.5,0.6);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 
	 public void testRINT() throws Exception{
		 	EvalFunc<DataAtom> RINT = new RINT();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, -0.5);
	        DataAtom output = new DataAtom();
	        RINT.exec(tup, output);
	        double expected = Math.rint(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testROUND() throws Exception{
		 	EvalFunc<DataAtom> ROUND = new ROUND();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        ROUND.exec(tup, output);
	        double expected = Math.round(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testSCALB() throws Exception {
	        EvalFunc<DataAtom> SCALB = new SCALB();
	        Tuple tup = new Tuple(2);
	        tup.setField(0, 1);
	        tup.setField(1,2);
	        DataAtom output = new DataAtom();
	        SCALB.exec(tup, output);
	        double expected = Math.scalb(1,2);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	    }
	 
	 public void testSIGNUM() throws Exception{
		 	EvalFunc<DataAtom> SIGNUM = new SIGNUM();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        SIGNUM.exec(tup, output);
	        double expected = Math.signum(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testSIN() throws Exception{
		 	EvalFunc<DataAtom> SIN = new SIN();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        SIN.exec(tup, output);
	        double expected = Math.sin(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testSINH() throws Exception{
		 	EvalFunc<DataAtom> SINH = new SINH();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        SINH.exec(tup, output);
	        double expected = Math.sinh(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testSQRT() throws Exception{
		 	EvalFunc<DataAtom> SQRT = new SQRT();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        SQRT.exec(tup, output);
	        double expected = Math.sqrt(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testTAN() throws Exception{
		 	EvalFunc<DataAtom> TAN = new TAN();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        TAN.exec(tup, output);
	        double expected = Math.tan(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testTANH() throws Exception{
		 	EvalFunc<DataAtom> TANH = new TANH();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        TANH.exec(tup, output);
	        double expected = Math.tanh(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testtoDegree() throws Exception{
		 	EvalFunc<DataAtom> toDegrees = new toDegrees();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        toDegrees.exec(tup, output);
	        double expected = Math.toDegrees(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testtoRadians() throws Exception{
		 	EvalFunc<DataAtom> toRadians = new toRadians();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        toRadians.exec(tup, output);
	        double expected = Math.toRadians(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
	 public void testULP() throws Exception{
		 	EvalFunc<DataAtom> ULP = new ULP();
	        Tuple tup = new Tuple(1);
	        tup.setField(0, 0.5);
	        DataAtom output = new DataAtom();
	        ULP.exec(tup, output);
	        double expected = Math.ulp(0.5);
	        double actual = (new Double(output.strval())).doubleValue();
	        assertEquals(actual, expected, delta);
	 }
	 
}
