/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.pigunit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;

/**
 * Pig Unit
 *
 * <p>Equivalent of xUnit for testing Pig.
 *
 * <p>Call {@link PigTest#getCluster()} then construct a test and call an assert method.
 * Have a look to the test of this class for more example.
 */
public class PigTest {
  /** The text of the Pig script to test with no substitution or change. */
  private final String originalTextPigScript;
  /** The list of arguments of the script. */
  private final String[] args;
  /** The list of file arguments of the script. */
  private final String[] argFiles;
  /** The list of aliases to override in the script. */
  private final Map<String, String> aliasOverrides;

  private static ThreadLocal<PigServer> pig = new ThreadLocal<PigServer>();
  private static ThreadLocal<Cluster> cluster = new ThreadLocal<Cluster>();

  private static final Logger LOG = Logger.getLogger(PigTest.class);
  private static final String EXEC_CLUSTER = "pigunit.exectype.cluster";

  /**
   * Initializes the Pig test.
   *
   * @param args The list of arguments of the script.
   * @param argFiles The list of file arguments of the script.
   * @param pigTextScript The text of the Pig script to test with no substitution or change.
   */
  @SuppressWarnings("serial")
  PigTest(String[] args, String[] argFiles, String pigTextScript) {
    this.originalTextPigScript = pigTextScript;
    this.args = args;
    this.argFiles = argFiles;
    this.aliasOverrides = new HashMap<String, String>() {{
      put("STORE", "");
      put("DUMP", "");
    }};
  }

  public PigTest(String scriptPath) throws IOException {
    this(null, null, readFile(scriptPath));
  }

  public PigTest(String[] script) {
    this(null, null, StringUtils.join(script, "\n"));
  }

  public PigTest(String scriptPath, String[] args) throws IOException {
    this(args, null, readFile(scriptPath));
  }

  public PigTest(String[] script, String[] args) {
    this(args, null, StringUtils.join(script, "\n"));
  }

  public PigTest(String[] script, String[] args, String[] argsFile) {
    this(args, argsFile, StringUtils.join(script, "\n"));
  }

  public PigTest(String scriptPath, String[] args, String[] argFiles) throws IOException {
    this(args, argFiles, readFile(scriptPath));
  }

  public PigTest(String scriptPath, String[] args, PigServer pig, Cluster cluster)
      throws IOException {
    this(args, null, readFile(scriptPath));
    PigTest.pig.set(pig);
    PigTest.cluster.set(cluster);
  }

  /**
   * Connects and starts if needed the PigServer.
   *
   * @return Reference to the Cluster in ThreadLocal.
   * @throws ExecException If the PigServer can't be started.
   */
  public static Cluster getCluster() throws ExecException {
    if (cluster.get() == null) {
      if (System.getProperties().containsKey(EXEC_CLUSTER)) {
        LOG.info("Using cluster mode");
        pig.set(new PigServer(ExecType.MAPREDUCE));
      } else {
        LOG.info("Using default local mode");
        pig.set(new PigServer(ExecType.LOCAL));
      }

      cluster.set(new Cluster(pig.get().getPigContext()));
    }

    return cluster.get();
  }

  /**
   * Return the PigServer.
   *
   * @return Reference to the PigServer in ThreadLocal.
   */
  public static PigServer getPigServer() {
    return pig.get();
  }

  /**
   * Registers a pig scripts with its variables substituted.
   *
   * @throws IOException If a temp file containing the pig script could not be created.
   * @throws ParseException The pig script could not have all its variables substituted.
   */
  protected void registerScript() throws IOException, ParseException {
    getCluster();

    BufferedReader reader = new BufferedReader(new StringReader(this.originalTextPigScript));
    PigContext context = getPigServer().getPigContext();

    String substitutedPig = context.doParamSubstitution(reader,
                                                        args == null ? null : Arrays.asList(args),
                                                        argFiles == null ? null : Arrays.asList(argFiles));
    LOG.info(substitutedPig);

    File f = File.createTempFile("tmp", "pigunit");
    PrintWriter pw = new PrintWriter(f);
    pw.println(substitutedPig);
    pw.close();

    String pigSubstitutedFile = f.getCanonicalPath();
    getPigServer().registerScript(pigSubstitutedFile, aliasOverrides);
  }

  /**
   * Executes the Pig script with its current overrides.
   *
   * @throws IOException If a temp file containing the pig script could not be created.
   * @throws ParseException The pig script could not have all its variables substituted.
   */
  public void runScript() throws IOException, ParseException {
    registerScript();
  }

  /**
   * Gets an iterator on the content of one alias of the script.
   *
   * <p>For now use a giant String in order to display all the differences in one time. It might not
   * work with giant expected output.
   * @throws ParseException If the Pig script could not be parsed.
   * @throws IOException If the Pig script could not be executed correctly.
   */
  public Iterator<Tuple> getAlias(String alias) throws IOException, ParseException {
    registerScript();
    return getPigServer().openIterator(alias);
  }
  
  /**
   * Gets an iterator on the content of one alias of a cached script. The script itself
   * must be already be registered with registerScript().
   */
  private Iterator<Tuple> getAliasFromCache(String alias) throws IOException, ParseException {
    return getPigServer().openIterator(alias);
  }

  /**
   * Gets an iterator on the content of the latest STORE alias of the script.
   *
   * @throws ParseException If the Pig script could not be parsed.
   * @throws IOException If the Pig script could not be executed correctly.
   */
  public Iterator<Tuple> getAlias() throws IOException, ParseException {
    registerScript();
    String alias = aliasOverrides.get("LAST_STORE_ALIAS");

    return getAliasFromCache(alias);
  }

  /**
   * Replaces the query of an aliases by another query.
   *
   * <p>For example:
   *
   * <pre>
   * B = FILTER A BY count > 5;
   * overridden with:
   * &lt;B, B = FILTER A BY name == 'Pig';&gt;
   * becomes
   * B = FILTER A BY name == 'Pig';
   * </pre>
   *
   * @param alias The alias to override.
   * @param query The new value of the alias.
   */
  public void override(String alias, String query) {
    aliasOverrides.put(alias, query);
  }

  public void unoverride(String alias) {
    aliasOverrides.remove(alias);
  }

  public void assertOutput(String[] expected) throws IOException, ParseException {
    registerScript();
    String alias = aliasOverrides.get("LAST_STORE_ALIAS");

    assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(getAliasFromCache(alias), "\n"));
  }

  public void assertOutput(String alias, String[] expected) throws IOException, ParseException {
    registerScript();

    assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(getAliasFromCache(alias), "\n"));
  }

  public void assertOutput(File expected) throws IOException, ParseException {
    registerScript();
    String alias = aliasOverrides.get("LAST_STORE_ALIAS");

    assertEquals(readFile(expected).replaceAll("\r\n", "\n"), StringUtils.join(getAliasFromCache(alias), "\n"));
  }

  public void assertOutput(String alias, File expected) throws IOException, ParseException {
    registerScript();

    assertEquals(readFile(expected).replaceAll("\r\n", "\n"), StringUtils.join(getAliasFromCache(alias), "\n"));
  }

  public void assertOutput(String aliasInput, String[] input, String alias, String[] expected)
      throws IOException, ParseException {
    assertOutput(aliasInput, input, alias, expected, "\\t");
  }

  public void assertOutput(String aliasInput, String[] input, String alias, String[] expected, String delimiter)
      throws IOException, ParseException {
    registerScript();

    StringBuilder sb = new StringBuilder();
    Schema.stringifySchema(sb, getPigServer().dumpSchema(aliasInput), DataType.TUPLE) ;

    final String destination = FileLocalizer.getTemporaryPath(getPigServer().getPigContext()).toString();
    getCluster().copyFromLocalFile(input, destination, true);
    override(aliasInput,
        String.format("%s = LOAD '%s' USING PigStorage('%s') AS %s;", aliasInput, destination, delimiter, sb.toString()));

    assertOutput(alias, expected);
  }

  protected void assertEquals(String expected, String current) {
    Assert.assertEquals(expected, current);
  }

  private static String readFile(String path) throws IOException {
    return readFile(new File(path));
  }

  private static String readFile(File file) throws IOException {
    FileInputStream stream = new FileInputStream(file);
    try {
      FileChannel fc = stream.getChannel();
      MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
      return Charset.defaultCharset().decode(bb).toString();
    }
    finally {
      stream.close();
    }
  }
}
