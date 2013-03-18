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

package org.apache.pig.test;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.scripting.groovy.GroovyUtils;
import org.junit.Test;

public class TestUDFGroovy {

  @Test
  public void testPigToGroovy() throws Exception {
    Object pigObject = Boolean.TRUE;
    Object groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Boolean);
    assertEquals(true, groovyObject);

    pigObject = Integer.valueOf(42);
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Integer);
    assertEquals(42, groovyObject);

    pigObject = Long.valueOf(0x100000000L);
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Long);
    assertEquals(0x100000000L, groovyObject);

    pigObject = Float.MIN_VALUE;
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Float);
    assertEquals(Float.MIN_VALUE, groovyObject);

    pigObject = Double.MAX_VALUE;
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Double);
    assertEquals(Double.MAX_VALUE, groovyObject);

    pigObject = "Dans le cochon tout est bon !";
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof String);
    assertEquals("Dans le cochon tout est bon !", groovyObject);

    pigObject = new DataByteArray("Surtout le jambon".getBytes("UTF-8"));
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof byte[]);
    assertNotSame(groovyObject, pigObject);
    assertArrayEquals("Surtout le jambon".getBytes("UTF-8"), (byte[]) groovyObject);

    pigObject = new org.joda.time.DateTime();
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof org.joda.time.DateTime);
    assertSame(groovyObject, pigObject);

    pigObject = tuple("a","b","c");
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof groovy.lang.Tuple);
    assertEquals(3, ((groovy.lang.Tuple) groovyObject).size());
    assertEquals("a", ((groovy.lang.Tuple) groovyObject).get(0));
    assertEquals("b", ((groovy.lang.Tuple) groovyObject).get(1));
    assertEquals("c", ((groovy.lang.Tuple) groovyObject).get(2));

    pigObject = bag(tuple("a"), tuple("b"));
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof groovy.lang.Tuple);
    assertEquals(2, ((groovy.lang.Tuple) groovyObject).size());
    assertEquals(2L, ((groovy.lang.Tuple) groovyObject).get(0));
    assertTrue(((groovy.lang.Tuple) groovyObject).get(1) instanceof Iterator);
    Iterator<groovy.lang.Tuple> iter = (Iterator) ((groovy.lang.Tuple) groovyObject).get(1);
    groovy.lang.Tuple t = iter.next();
    assertEquals(1, t.size());
    assertEquals("a", t.get(0));
    t = iter.next();
    assertEquals(1, t.size());
    assertEquals("b", t.get(0));

    pigObject = new HashMap<String, String>();
    ((Map) pigObject).put("Pate", "Henaff");
    ((Map) pigObject).put("Rillettes", "Bordeau Chesnel");
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertTrue(groovyObject instanceof Map);
    assertEquals(2, ((Map) groovyObject).size());
    assertEquals("Henaff", ((Map) groovyObject).get("Pate"));
    assertEquals("Bordeau Chesnel", ((Map) groovyObject).get("Rillettes"));

    pigObject = BigInteger.ONE;
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertSame(pigObject, groovyObject);

    pigObject = new BigDecimal("42.42");
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertSame(pigObject, groovyObject);

    pigObject = null;
    groovyObject = GroovyUtils.pigToGroovy(pigObject);
    assertNull(groovyObject);
  }

  @Test
  public void testGroovyToPig() throws Exception {
    Object groovyObject = Boolean.TRUE;
    Object pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Boolean);
    assertEquals(true, pigObject);

    groovyObject = Byte.MIN_VALUE;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Integer);
    assertEquals((int) Byte.MIN_VALUE, pigObject);

    groovyObject = Short.MIN_VALUE;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Integer);
    assertEquals((int) Short.MIN_VALUE, pigObject);

    groovyObject = Integer.MIN_VALUE;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Integer);
    assertEquals(Integer.MIN_VALUE, pigObject);

    groovyObject = Long.MIN_VALUE;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Long);
    assertEquals(Long.MIN_VALUE, pigObject);

    groovyObject = BigInteger.TEN;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertSame(groovyObject, pigObject);

    groovyObject = Float.MIN_NORMAL;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Float);
    assertEquals(Float.MIN_NORMAL, pigObject);

    groovyObject = Double.MIN_VALUE;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Double);
    assertEquals(Double.MIN_VALUE, pigObject);

    groovyObject = new BigDecimal("42.42");
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertSame(groovyObject, pigObject);

    groovyObject = "Dans le cochon tout est bon !";
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof String);
    assertEquals("Dans le cochon tout est bon !", pigObject);

    groovyObject = "Surtout le jambon".getBytes("UTF-8");
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof DataByteArray);
    assertArrayEquals("Surtout le jambon".getBytes("UTF-8"), ((DataByteArray) pigObject).get());

    groovyObject = new Object[2];
    ((Object[]) groovyObject)[0] = "Pate";
    ((Object[]) groovyObject)[1] = "Henaff";
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Tuple);
    assertEquals(2, ((Tuple) pigObject).size());
    assertEquals("Pate", ((Tuple) pigObject).get(0));
    assertEquals("Henaff", ((Tuple) pigObject).get(1));

    groovyObject = new Object[2];
    ((Object[]) groovyObject)[0] = "Rillettes";
    ((Object[]) groovyObject)[1] = "Bordeau Chesnel";
    groovyObject = new groovy.lang.Tuple((Object[]) groovyObject);
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Tuple);
    assertEquals(2, ((Tuple) pigObject).size());
    assertEquals("Rillettes", ((Tuple) pigObject).get(0));
    assertEquals("Bordeau Chesnel", ((Tuple) pigObject).get(1));

    groovyObject = new ArrayList<Object>();
    ((List<Object>) groovyObject).add("Jaret");
    ((List<Object>) groovyObject).add("Filet Mignon");
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof DataBag);
    assertEquals(2, ((DataBag) pigObject).size());
    Iterator<Tuple> iter = ((DataBag) pigObject).iterator();
    Set<String> values = new HashSet<String>();
    while (iter.hasNext()) {
      Tuple t = iter.next();
      assertEquals(1, t.size());
      values.add((String) t.get(0));
    }
    assertEquals(2, values.size());
    assertTrue(values.contains("Jaret"));
    assertTrue(values.contains("Filet Mignon"));

    groovyObject = new HashMap<String, String>();
    ((Map) groovyObject).put("Henaff", "a bord");
    ((Map) groovyObject).put("Copains", "comme cochons");
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof Map);
    assertEquals(2, ((Map) pigObject).size());
    assertEquals("a bord", ((Map) pigObject).get("Henaff"));
    assertEquals("comme cochons", ((Map) pigObject).get("Copains"));

    groovyObject = TupleFactory.getInstance().newTuple(2);
    ((Tuple) groovyObject).set(0, "jambon");
    ((Tuple) groovyObject).set(1, "blanc");
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertSame(groovyObject, pigObject);

    groovyObject = new DefaultDataBag();
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertSame(groovyObject, pigObject);

    groovyObject = new org.joda.time.DateTime();
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertTrue(pigObject instanceof org.joda.time.DateTime);
    assertSame(groovyObject, pigObject);

    groovyObject = null;
    pigObject = GroovyUtils.groovyToPig(groovyObject);
    assertNull(pigObject);
  }

  @Test
  public void testEvalFunc_Static() throws Exception {
    String[] groovyStatements = {
        "import org.apache.pig.builtin.OutputSchema;",
        "class GroovyUDF {",
        "  @OutputSchema('x:long')",
        "  static long square(long x) {",
        "    return x*x;",
        "  }",
        "}"
    };

    File tmpScriptFile = File.createTempFile("temp_groovy_udf", ".groovy");
    tmpScriptFile.deleteOnExit();
    FileWriter writer = new FileWriter(tmpScriptFile);
    for (String line : groovyStatements) {
      writer.write(line + "\n");
    }
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);

    pigServer.registerCode(tmpScriptFile.getCanonicalPath(), "groovy", "groovyudfs");

    Data data = resetData(pigServer);
    data.set("foo0",
        tuple(1),
        tuple(2),
        tuple(3),
        tuple(4)
        );

    pigServer.registerQuery("A = LOAD 'foo0' USING mock.Storage();");
    pigServer.registerQuery("B = FOREACH A GENERATE groovyudfs.square($0);");
    pigServer.registerQuery("STORE B INTO 'bar0' USING mock.Storage();");

    List<Tuple> out = data.get("bar0");
    assertEquals(tuple(1L), out.get(0));
    assertEquals(tuple(4L), out.get(1));
    assertEquals(tuple(9L), out.get(2));
    assertEquals(tuple(16L), out.get(3));
  }

  @Test
  public void testEvalFunc_NonStatic() throws Exception {
    String[] groovyStatements = {
        "import org.apache.pig.builtin.OutputSchema;",
        "class GroovyUDF {",
        "  private final long multiplicator;",
        "  public GroovyUDF() {",
        "    this.multiplicator = 42L;",
        "  }",
        "  @OutputSchema('x:long')",
        "  long mul(long x) {",
        "    return x*this.multiplicator;",
        "  }",
        "}"
    };

    File tmpScriptFile = File.createTempFile("temp_groovy_udf", ".groovy");
    tmpScriptFile.deleteOnExit();
    FileWriter writer = new FileWriter(tmpScriptFile);
    for (String line : groovyStatements) {
      writer.write(line + "\n");
    }
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);

    pigServer.registerCode(tmpScriptFile.getCanonicalPath(), "groovy", "groovyudfs");

    Data data = resetData(pigServer);
    data.set("foo1",
        tuple(1)
        );

    pigServer.registerQuery("A = LOAD 'foo1' USING mock.Storage();");
    pigServer.registerQuery("B = FOREACH A GENERATE groovyudfs.mul($0);");
    pigServer.registerQuery("STORE B INTO 'bar1' USING mock.Storage();");

    List<Tuple> out = data.get("bar1");
    assertEquals(tuple(42L), out.get(0));
  }

  @Test
  public void testAlgebraicEvalFunc() throws Exception {
    String[] groovyStatements = {
        "import org.apache.pig.scripting.groovy.AlgebraicInitial;",
        "import org.apache.pig.scripting.groovy.AlgebraicIntermed;",
        "import org.apache.pig.scripting.groovy.AlgebraicFinal;",
        "class GroovyUDFs {",
        "  @AlgebraicFinal('sumalg')",
        "  public static long algFinal(Tuple t) {",
        "    long x = 0;",
        "    for (Object o: t[1]) {",
        "      x = x + o;",
        "    }",
        "    return x;",
        "  }",
        "  @AlgebraicInitial('sumalg')",
        "  public static Tuple algInitial(Tuple t) {",
        "    long x = 0;",
        "    for (Object o: t[1]) {",
        "      x = x + o[0];",
        "    }",
        "    return [x];",
        "  }",
        "  @AlgebraicIntermed('sumalg')",
        "  public static Tuple algIntermed(Tuple t) {",
        "    long x = 0;",
        "    for (Object o: t[1]) {",
        "      x = x + o;",
        "    }",
        "    return [x];",
        "  }",
        "}"
    };

    File tmpScriptFile = File.createTempFile("temp_groovy_udf", ".groovy");
    tmpScriptFile.deleteOnExit();
    FileWriter writer = new FileWriter(tmpScriptFile);
    for (String line : groovyStatements) {
      writer.write(line + "\n");
    }
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);

    pigServer.registerCode(tmpScriptFile.getCanonicalPath(), "groovy", "groovyudfs");

    Data data = resetData(pigServer);
    data.set("foo2",
        tuple(1),
        tuple(2),
        tuple(3),
        tuple(4)
        );

    pigServer.registerQuery("A = LOAD 'foo2' USING mock.Storage();");
    pigServer.registerQuery("B = GROUP A ALL;");
    pigServer.registerQuery("C = FOREACH B GENERATE groovyudfs.sumalg(A);");
    pigServer.registerQuery("STORE C INTO 'bar2' USING mock.Storage();");

    List<Tuple> out = data.get("bar2");
    assertEquals(tuple(10L), out.get(0));
  }

  @Test
  public void testAccumulatorEvalFunc() throws Exception {
    String[] groovyStatements = {
        "import org.apache.pig.builtin.OutputSchema;",
        "import org.apache.pig.scripting.groovy.AccumulatorAccumulate;",
        "import org.apache.pig.scripting.groovy.AccumulatorGetValue;",
        "import org.apache.pig.scripting.groovy.AccumulatorCleanup;",
        "class GroovyUDFs {",
        "  private int sum = 0;",
        "  @AccumulatorAccumulate('sumacc')",
        "  public void accuAccumulate(Tuple t) {",
        "    for (Object o: t[1]) {",
        "      sum += o[0]",
        "    }",
        "  }",
        "  @AccumulatorGetValue('sumacc')",
        "  @OutputSchema('sum: long')",
        "  public long accuGetValue() {",
        "    return this.sum;",
        "  }",
        "  @AccumulatorCleanup('sumacc')",
        "  public void accuCleanup() {",
        "    this.sum = 0L;",
        "  }",
        "}"
    };

    File tmpScriptFile = File.createTempFile("temp_groovy_udf", ".groovy");
    tmpScriptFile.deleteOnExit();
    FileWriter writer = new FileWriter(tmpScriptFile);
    for (String line : groovyStatements) {
      writer.write(line + "\n");
    }
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);

    pigServer.registerCode(tmpScriptFile.getCanonicalPath(), "groovy", "groovyudfs");

    Data data = resetData(pigServer);
    data.set("foo3",
        tuple(1),
        tuple(2),
        tuple(3),
        tuple(4)
        );

    pigServer.registerQuery("A = LOAD 'foo3' USING mock.Storage();");
    pigServer.registerQuery("B = GROUP A ALL;");
    pigServer.registerQuery("C = FOREACH B GENERATE groovyudfs.sumacc(A) AS sum1,groovyudfs.sumacc(A) AS sum2;");
    pigServer.registerQuery("STORE C INTO 'bar3' USING mock.Storage();");

    List<Tuple> out = data.get("bar3");
    assertEquals(tuple(10L,10L), out.get(0));
  }

  @Test
  public void testOutputSchemaFunction() throws Exception {
    String[] groovyStatements = {
        "import org.apache.pig.scripting.groovy.OutputSchemaFunction;",
        "class GroovyUDFs {",
        "  @OutputSchemaFunction('squareSchema')",
        "  public static square(x) {",
        "    return x * x;",
        "  }",
        "  public static squareSchema(input) {",
        "    return input;",
        "  }",
        "}"
    };

    File tmpScriptFile = File.createTempFile("temp_groovy_udf", ".groovy");
    tmpScriptFile.deleteOnExit();
    FileWriter writer = new FileWriter(tmpScriptFile);
    for (String line : groovyStatements) {
      writer.write(line + "\n");
    }
    writer.close();

    PigServer pigServer = new PigServer(ExecType.LOCAL);

    pigServer.registerCode(tmpScriptFile.getCanonicalPath(), "groovy", "groovyudfs");

    Data data = resetData(pigServer);
    data.set("foo4",
        tuple(1,1L,1.0F,1.0D),
        tuple(2,2L,2.0F,2.0D)
        );

    pigServer.registerQuery("A = LOAD 'foo4' USING mock.Storage() AS (i: int, l: long, f: float, d: double);");
    pigServer.registerQuery("B = FOREACH A GENERATE groovyudfs.square(i),groovyudfs.square(l),groovyudfs.square(f),groovyudfs.square(d);");
    pigServer.registerQuery("STORE B INTO 'bar4' USING mock.Storage();");

    List<Tuple> out = data.get("bar4");
    // Multiplying two floats leads to a double in Groovy, this is reflected here.
    assertEquals(tuple(1,1L,1.0D,1.0D), out.get(0));
    assertEquals(tuple(4,4L,4.0D,4.0D), out.get(1));
  }
}
