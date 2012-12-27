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
package org.apache.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestAlgebraicEvalWithParameterizedReturnType {
  public static class AlgebraicEvalFuncWithParameterizedReturnType extends
      EvalFunc<Map<String, Long>> implements Algebraic {
    public static class Initial extends EvalFunc<Tuple> {
      @Override
      public Tuple exec(Tuple input) throws IOException {
        return null;
      }
    }

    public static class Intermediate extends EvalFunc<Tuple> {
      @Override
      public Tuple exec(Tuple input) throws IOException {
        return null;
      }
    }

    public static class Final extends EvalFunc<Map<String, Long>> {
      @Override
      public Map<String, Long> exec(Tuple input) throws IOException {
        return null;
      }
    }

    @Override
    public Map<String, Long> exec(Tuple input) throws IOException {
      return null;
    }

    @Override
    public String getInitial() {
      return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
      return Intermediate.class.getName();
    }

    @Override
    public String getFinal() {
      return Final.class.getName();
    }
  }

  @Test
  public void testConstruction() {
    new AlgebraicEvalFuncWithParameterizedReturnType();
  }
}
