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

import org.apache.pig.test.MiniCluster;


/**
 * Starts an on-demand mini cluster that requires no set up.
 *
 * <p>It can be useful if you don't want to restart the cluster between each run of test and don't
 * want to set up a real cluster.
 *
 * <p>CLASSPATH needs to contain: pig.jar and piggybank.jar
 * <pre>
 * export CLASSPATH=/path/pig.jar:/path/piggybank.jar
 * java org.apache.pig.pigunit.MiniClusterRunner
 * </pre>
 *
 * <p>Possible improvements
 * <ul>
 *   <li>add a main in MiniCluster</li>
 *   <li>make MiniCluster configurable (number of maps...)</li>
 *   <li>make MiniCluster use a default properties for chosing the hadoop conf dir
 *       (e.g. minicluster.conf.dir) instead of always using
 *       System.getProperty("user.home"), "pigtest/conf/"</li>
 *   <li>use CLI option</li>
 *   <li>make a shell wrapper</li>
 * </ul>
 */
public class MiniClusterRunner {
  public static void main(String[] args) {
    System.setProperty("hadoop.log.dir", "/tmp/pigunit");
    MiniCluster.buildCluster();
  }
}
