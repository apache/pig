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
package org.apache.pig.penny.impl.pig;

import java.io.File;

import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.impl.harnesses.CoordinatorHarness;
import org.apache.pig.tools.ToolsPigServer;


public class PigLauncher {

  // Ibis change : start
  // Allow to customize location of penny.jar instead of always relying on cwd.
    private final static String pennyJarFilename = System.getProperty("PENNY_JAR", "penny.jar");
  // Ibis change : end
    
    public static Object launch(PigContext pigContext, LogicalPlan queryPlan, ClassWithArgs coordinatorClass) throws Exception {
        Coordinator coord = (Coordinator) coordinatorClass.theClass().newInstance();
        CoordinatorHarness coordHarness = new CoordinatorHarness(coord);
        coord.init(coordinatorClass.args());
        ToolsPigServer pigServer = new ToolsPigServer(pigContext);
        
        if (!(new File(pennyJarFilename)).exists()) {
            throw new RuntimeException("Cannot find " + pennyJarFilename + " in working directory.");
        }
        pigServer.registerJar(pennyJarFilename);

        pigServer.runPlan(queryPlan, "penny");
        return coordHarness.finish();
    }
    
}
