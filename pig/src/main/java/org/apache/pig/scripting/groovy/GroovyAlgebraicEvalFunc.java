/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.scripting.groovy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public abstract class GroovyAlgebraicEvalFunc<T> extends AlgebraicEvalFunc<T> {

  public GroovyAlgebraicEvalFunc() {
  }

  public GroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
      String finalMethod) {
    super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
  }

  @Override
  public abstract String getFinal();

  @Override
  public String getInitial() {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed() {
    return Intermed.class.getName();
  }

  public static abstract class AlgebraicFunctionWrapper<T> extends GroovyEvalFunc<T> {
    public AlgebraicFunctionWrapper() {
    }

    public AlgebraicFunctionWrapper(String path, String namespace, String methodName) throws IOException {
      super(path, namespace, methodName);
    }
  }

  public static class Initial extends AlgebraicFunctionWrapper<Tuple> {
    public Initial() {
    }

    public Initial(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
        String finalMethod) throws IOException {
      super(path, namespace, initialMethod);
    }
  }

  public static class Intermed extends AlgebraicFunctionWrapper<Tuple> {
    public Intermed() {
    }

    public Intermed(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
        String finalMethod) throws IOException {
      super(path, namespace, intermedMethod);
    }
  }

  public static class Final<T> extends AlgebraicFunctionWrapper<T> {
    public Final() {
    }

    public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
        String finalMethod) throws IOException {
      super(path, namespace, finalMethod);
    }
  }

  /**
   * Unlike EvalFuncs and Accumulators, the type must be known at compile time
   * (ie it
   * can't return Object), as Pig inspects the type and ensures that it is
   * valid. This
   * is why class specific shells are provided here.
   */
  public static class DataBagGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<DataBag> {
    public DataBagGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<DataBag> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class TupleGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Tuple> {
    public TupleGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Tuple> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class ChararrayGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<String> {
    public ChararrayGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<String> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class DataByteArrayGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<DataByteArray> {
    public DataByteArrayGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<DataByteArray> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class DateTimeGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<org.joda.time.DateTime> {
    public DateTimeGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<org.joda.time.DateTime> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class DoubleGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Double> {
    public DoubleGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Double> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class FloatGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Float> {
    public FloatGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Float> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class IntegerGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Integer> {
    public IntegerGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Integer> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class LongGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Long> {
    public LongGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Long> {
      public Final() {
      }

      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class MapGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Map<String, ?>> {
    public MapGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Map<String, ?>> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class BooleanGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<Boolean> {
    public BooleanGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<Boolean> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class BigIntegerGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<BigInteger> {
    public BigIntegerGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<BigInteger> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }

  public static class BigDecimalGroovyAlgebraicEvalFunc extends GroovyAlgebraicEvalFunc<BigDecimal> {
    public BigDecimalGroovyAlgebraicEvalFunc(String path, String namespace, String methodName, String initialMethod,
        String intermedMethod, String finalMethod) throws IOException {
      super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }

    public static class Final extends GroovyAlgebraicEvalFunc.Final<BigDecimal> {
      public Final(String path, String namespace, String methodName, String initialMethod, String intermedMethod,
          String finalMethod) throws IOException {
        super(path, namespace, methodName, initialMethod, intermedMethod, finalMethod);
      }
    }
  }
}
