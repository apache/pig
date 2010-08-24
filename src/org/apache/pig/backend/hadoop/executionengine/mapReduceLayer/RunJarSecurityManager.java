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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.security.Permission;

public class RunJarSecurityManager extends SecurityManager{
    private boolean exitInvoked = false;
    private int exitCode;
    private SecurityManager securityManager = null;
    private boolean internal = true;
    
    public RunJarSecurityManager() {
        // install only once
        if(securityManager == null) {
            securityManager = System.getSecurityManager();
            System.setSecurityManager(this);
        }
    }   

    @Override
    public void checkPermission(Permission perm) {
        if (securityManager != null) {
            // check everything with the original SecurityManager
            securityManager.checkPermission(perm);
        }
    }
    
    public void retire() {
        System.setSecurityManager(securityManager);
    }

    @Override
    public void checkExit(int status) throws SecurityException {
        // We need internal because RunJar intercepts all the exceptions and then itself returns -1
        // So it also intercepts our SecurityException, to avoid this we only take the first System.exit, the real source
        if(internal) {
            exitInvoked = true;
            exitCode = status;
            internal = false;
        }
        throw new SecurityException("Intercepted System.exit(" + status + ")");
    }   

    public boolean getExitInvoked() {
        return exitInvoked;
    }   

    public int getExitCode() {
        return exitCode;
    } 
    
}
