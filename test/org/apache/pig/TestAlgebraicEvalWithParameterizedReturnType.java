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
