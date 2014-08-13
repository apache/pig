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
package org.apache.pig.pigunit.pig;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.Utils;

/**
 * Slightly modified PigServer that accepts a list of Pig aliases to override.
 *
 * <p>The list is given to the GruntParser.
 */
public class PigServer extends org.apache.pig.PigServer {

  public PigServer(ExecType execType, Properties properties) throws ExecException {
    super(execType, properties);
  }

  public PigServer(ExecType execType) throws ExecException {
    super(execType);
  }

  /**
   * Parses and registers the pig script.
   *
   * @param fileName The Pig script file.
   * @param aliasOverride The list of aliases to override in the Pig script.
   * @throws IOException If the Pig script can't be parsed correctly.
   */
  public void registerScript(String fileName, Map<String, String> aliasOverride)
      throws IOException {
    try {
      InputStream compositeStream = Utils.getCompositeStream(new FileInputStream(fileName), pigContext.getProperties());
      GruntParser grunt = new GruntParser(new InputStreamReader(compositeStream), this, aliasOverride);
      grunt.setInteractive(false);
      grunt.parseStopOnError(true);
    } catch (FileNotFoundException e) {
      throw new IOException(e);
    } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
      throw new IOException(e);
    }
  }
}
