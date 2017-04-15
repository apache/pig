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
import java.lang.Object;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.ExecTypeProvider;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
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
  private static final String EXEC_CLUSTER = "pigunit.exectype";

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
    try {
      if (cluster.get() == null) {
        ExecType execType = ExecType.LOCAL;
        if (System.getProperties().containsKey(EXEC_CLUSTER)) {
          if (System.getProperties().getProperty(EXEC_CLUSTER).equalsIgnoreCase("mr")) {
            LOG.info("Using mr cluster mode");
            execType = ExecType.MAPREDUCE;
          } else if (System.getProperties().getProperty(EXEC_CLUSTER).equalsIgnoreCase("tez")) {
            LOG.info("Using tez cluster mode");
            execType = ExecTypeProvider.fromString("tez");
          } else if (System.getProperties().getProperty(EXEC_CLUSTER).equalsIgnoreCase("tez_local")) {
            LOG.info("Using tez local mode");
            execType = ExecTypeProvider.fromString("tez_local");
          } else {
            LOG.info("Using default local mode");
          }
        } else {
          LOG.info("Using default local mode");
        }
        pig.set(new PigServer(execType));
        cluster.set(new Cluster(pig.get().getPigContext()));
      }
    } catch (PigException e) {
      throw new ExecException(e);
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

  /**
   * Returns a Map that has alias to it's schema.
   *
   * @return A map that has alias name as a key and the alias's schema as a value
   * @throws FrontendException If there was an error dumping the schema
   */
  public Map<String, String> getAliasToSchemaMap() throws FrontendException, IOException, ParseException {
    HashMap<String, String> aliasSchemas = new HashMap<String, String>();
    registerScript();
    PigServer server = getPigServer();
    Set<String> aliasKeySet = server.getAliasKeySet();
    for (String alias: aliasKeySet) {
      try {
        StringBuilder tsb = new StringBuilder();
        Schema.stringifySchema(tsb, server.dumpSchema(alias), DataType.TUPLE, Integer.MIN_VALUE);
        aliasSchemas.put(alias, tsb.toString());
      } catch (FrontendException e) {
        /**
         * If PigServer fails to describe a schema for an alias a FrontendException is thrown.
         * PigServer.getAliasKeySet() returns aliases that cannot have their schema described.
         * We want to skip over these particular aliases.
         */
        if (e.getErrorCode() == 1001) {
          //Let's print a warning
          System.out.println(e.getMessage());
        } else {
          throw e;
        }
      }
    }
    return aliasSchemas;
  }

  /**
   * Creates a temp file and populates it with the specified mock data.
   *
   * @param alias alias that the temp file is for
   * @param mockData data that is being mocked for the alias
   * @return path to the temp file
   */
  private String makeMockTempFile(String alias, String[] mockData) throws IOException {
    //The FileLocalizer uses a random variable, but that can have collisions. By using the current time, thread,
    //and alias we should be able to guaratee a unique file
    String uniqueSuffix = alias + "." + System.currentTimeMillis() + "." + Thread.currentThread().getId();
    //PigServer/Cluster is not initialized yet. Let's initialize it.
    if (getPigServer() == null) {
        getCluster();
    }
    String path = FileLocalizer.getTemporaryPath(getPigServer().getPigContext(), uniqueSuffix).toString();
    getCluster().copyFromLocalFile(mockData, path, true);
    return path;
  }

  private String getActualResults(String alias, boolean ignoreOrder) throws IOException, ParseException {
    //Tuples are sortable, but we should sort it as Strings for convenience when comparing to expected data
    Iterator<Tuple> iterator = getAliasFromCache(alias);
    List<String> actualResults = new ArrayList<String>();
    while (iterator.hasNext()) {
      actualResults.add(iterator.next().toString());
    }
    
    if (ignoreOrder) {
      Collections.sort(actualResults);
    }
    return StringUtils.join(actualResults, "\n");
  }

  /**
   * Allows you to mock a specific alias.
   *
   * This method will create a temporary file on the system that contains the mock data.  It will then change the pig
   * script to actually replace the alias when being assigned a value with a load file command which loads the temporary
   * file.
   *
   * @param alias The alias to be mocked
   * @param mockData The data you wished to be contained in the alias where each element in the array is a tab-delimited
   * @param aliasSchema The schema of the alias provided. Your mockData should fit this schema
   */
  public void mockAlias(String alias, String[] mockData, String aliasSchema) throws IOException {
    mockAlias(alias, mockData, aliasSchema, "\\t");
  }

  /**
   * Allows you to mock a specific alias.
   *
   * This method will create a temporary file on the system that contains the mock data.  It will then change the pig
   * script to actually replace the alias when being assigned a value with a load file command which loads the temporary
   * file.
   *
   * @param alias The alias to be mocked
   * @param mockData String array where each element is an entry in the alias and the line has its elements delimited
   *                 by the value provided by delimiter.
   * @param aliasSchema This is the schema of the alias being mocked
   * @param delimiter The delimiter used to separate data in mockData
   */
  public void mockAlias(String alias, String[] mockData, String aliasSchema, String delimiter) throws IOException {
    String mockFile = makeMockTempFile(alias, mockData);
    override(alias, String.format("%s = LOAD '%s' USING PigStorage('%s') AS %s;", alias, mockFile, delimiter, aliasSchema));
  }
  
  /**
   * Compares the expected results to the results of the last alias generated in the script. Order does not matter
   * and as long as the result is located in any index of expected and any line of the output then this will pass.
   * 
   * @param expected The expected results
   */
  public void assertOutputAnyOrder(String[] expected) throws IOException, ParseException {
    assertOutput(expected, true);
  }
  
  public void assertOutput(String[] expected) throws IOException, ParseException {
    assertOutput(expected, false);
  }

  private void assertOutput(String[] expected, boolean ignoreOrder) throws IOException, ParseException {
    registerScript();
    String alias = aliasOverrides.get("LAST_STORE_ALIAS");

    if (ignoreOrder) {
      Arrays.sort(expected);
    }
    assertEquals(StringUtils.join(expected, "\n"), getActualResults(alias, ignoreOrder));
  }
  
  /**
   * Compares the expected results to the results of the provided alias's output. Order does not matter
   * and as long as the result is located in any index of expected and any line of the output then this will pass.
   * 
   * @param alias The alias whose results we want to check
   * @param expected The expected results
   */
  public void assertOutputAnyOrder(String alias, String[] expected) throws IOException, ParseException {
    assertOutput(alias, expected, true);
  }
  
  public void assertOutput(String alias, String[] expected) throws IOException, ParseException {
    assertOutput(alias, expected, false);
  }

  private void assertOutput(String alias, String[] expected, boolean ignoreOrder) throws IOException, ParseException {
    registerScript();

    if (ignoreOrder) {
      Arrays.sort(expected);
    }
    assertEquals(StringUtils.join(expected, "\n"), getActualResults(alias, ignoreOrder));
  }

  public void assertOutput(File expected) throws IOException, ParseException {
    assertOutput(readFile(expected).split("(\\r\\n|\\n)"), false);
  }
  
  public void assertOutput(String alias, File expected) throws IOException, ParseException {
    assertOutput(alias, readFile(expected).split("(\\r\\n|\\n)"), false);
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

    mockAlias(aliasInput, input, sb.toString(), delimiter);

    assertOutput(alias, expected, false);
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
