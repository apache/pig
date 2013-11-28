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

import org.apache.pig.piggybank.evaluation.math.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;

import junit.framework.TestCase;

public class TestMathUDF extends TestCase{
    public double delta = 0.001;

     public void testABS() throws Exception {
            ABS abs = new ABS();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, -1.0);}catch(Exception e){}
            Double actual = abs.exec(tup);
            double expected = 1.0;
            assertEquals(actual, expected, delta);
        }

     public void testACOS() throws Exception {
            ACOS acos = new ACOS();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = acos.exec(tup);
            double expected = Math.acos(0.5);
            assertEquals(actual, expected, delta);
        }

     public void testASIN() throws Exception {
            ASIN asin = new ASIN();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = asin.exec(tup);
            double expected = Math.asin(0.5);
            assertEquals(actual, expected, delta);
        }

     public void testATAN() throws Exception {
            ATAN atan = new ATAN();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = atan.exec(tup);
            double expected = Math.atan(0.5);
            assertEquals(actual, expected, delta);
        }

     public void testATAN2() throws Exception {
            ATAN2 atan2 = new ATAN2();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, 0.5);
                tup.set(1,0.6);
            }catch(Exception e){}

            Double actual = atan2.exec(tup);
            double expected = Math.atan2(0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testCBRT() throws Exception{
             CBRT cbrt = new CBRT();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = cbrt.exec(tup);
            double expected = Math.cbrt(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testCEIL() throws Exception{
             CEIL ceil = new CEIL();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = ceil.exec(tup);
            double expected = Math.ceil(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testcopySign() throws Exception {
            copySign cpSign = new copySign();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch(Exception e){}
            Double actual = cpSign.exec(tup);
            double expected = Math.copySign(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testCOS() throws Exception{
             COS cos = new COS();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = cos.exec(tup);
            double expected = Math.cos(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testCOSH() throws Exception{
             COSH cosh = new COSH();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = cosh.exec(tup);
            double expected = Math.cosh(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testEXP() throws Exception{
             EXP exp = new EXP();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = exp.exec(tup);
            double expected = Math.exp(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testEXPM1() throws Exception{
             EXPM1 expm1 = new EXPM1();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = expm1.exec(tup);
            double expected = Math.expm1(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testFLOOR() throws Exception{
             FLOOR floor = new FLOOR();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = floor.exec(tup);
            double expected = Math.floor(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testgetExponent() throws Exception{
             getExponent getExp = new getExponent();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Integer actual = getExp.exec(tup);
            int expected = Math.getExponent(-0.5);
            assertEquals((int)actual, expected);
     }

     public void testHYPOT() throws Exception {
            HYPOT hypot = new HYPOT();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = hypot.exec(tup);
            double expected = Math.hypot(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testIEEEremainder() throws Exception {
            IEEEremainder rem = new IEEEremainder();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = rem.exec(tup);
            double expected = Math.IEEEremainder(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testLOG() throws Exception{
             LOG  log= new LOG();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = log.exec(tup);
            double expected = Math.log(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testLOG10() throws Exception{
             LOG10 log10 = new LOG10();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = log10.exec(tup);
            double expected = Math.log10(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testLOG1P() throws Exception{
             LOG1P log1p = new LOG1P();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = log1p.exec(tup);
            double expected = Math.log1p(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testMAX() throws Exception {
            MAX max = new MAX();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = max.exec(tup);
            double expected = Math.max(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testMIN() throws Exception {
            MIN min = new MIN();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = min.exec(tup);
            double expected = Math.min(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testnextAfter() throws Exception {
            nextAfter next = new nextAfter();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, -0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = next.exec(tup);
            double expected = Math.nextAfter(-0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testPOW() throws Exception {
            POW pow = new POW();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, 0.5);
                tup.set(1,0.6);
            }catch (Exception e){}
            Double actual = pow.exec(tup);
            double expected = Math.pow(0.5,0.6);
            assertEquals(actual, expected, delta);
        }

     public void testRINT() throws Exception{
             RINT rint = new RINT();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = rint.exec(tup);
            double expected = Math.rint(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testROUND() throws Exception{
             ROUND round = new ROUND();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Long actual = round.exec(tup);
            long expected = Math.round(0.5);
            assertEquals((long)actual, expected);
     }

     public void testSCALB() throws Exception {
            SCALB scalb = new SCALB();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(2);
            try{
                tup.set(0, 1);
                tup.set(1, 2);
            }catch (Exception e){}
            Double actual = scalb.exec(tup);
            double expected = Math.scalb(1,2);
            assertEquals(actual, expected, delta);
        }

     public void testSIGNUM() throws Exception{
             SIGNUM signum = new SIGNUM();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = signum.exec(tup);
            double expected = Math.signum(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testSIN() throws Exception{
             SIN sin = new SIN();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = sin.exec(tup);
            double expected = Math.sin(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testSINH() throws Exception{
             SINH sinh = new SINH();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = sinh.exec(tup);
            double expected = Math.sinh(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testSQRT() throws Exception{
             SQRT sqrt = new SQRT();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = sqrt.exec(tup);
            double expected = Math.sqrt(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testTAN() throws Exception{
             TAN tan = new TAN();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = tan.exec(tup);
            double expected = Math.tan(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testTANH() throws Exception{
             TANH tanh = new TANH();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = tanh.exec(tup);
            double expected = Math.tanh(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testtoDegree() throws Exception{
             toDegrees degree = new toDegrees();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = degree.exec(tup);
            double expected = Math.toDegrees(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testtoRadians() throws Exception{
             toRadians radian = new toRadians();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = radian.exec(tup);
            double expected = Math.toRadians(0.5);
            assertEquals(actual, expected, delta);
     }

     public void testULP() throws Exception{
             ULP ulp = new ULP();
            Tuple tup = DefaultTupleFactory.getInstance().newTuple(1);
            try{tup.set(0, 0.5);}catch(Exception e){}
            Double actual = ulp.exec(tup);
            double expected = Math.ulp(0.5);
            assertEquals(actual, expected, delta);
     }

}
