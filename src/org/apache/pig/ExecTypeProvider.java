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

import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.LocalExecType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecType;
import org.apache.pig.impl.util.PropertiesUtil;

public class ExecTypeProvider {

    private static final Log log = LogFactory.getLog(ExecTypeProvider.class);

    public static ExecType selectExecType(Properties properties)
            throws PigException {

        ServiceLoader<ExecType> frameworkLoader = ServiceLoader
                .load(ExecType.class);

        for (ExecType execType : frameworkLoader) {
            log.info("Trying ExecType : " + execType);
            if (execType.accepts(properties)) {
                log.info("Picked " + execType + " as the ExecType");
                return getSingleton(execType);
            } else {
                log.debug("Cannot pick " + execType + " as the ExecType");
            }
        }
        throw new PigException("Unknown exec type: "
                + properties.getProperty("exectype"), 2040);
    }

    /**
     * This method attempts to return a singleton instance of the given exec
     * type. Only works for MR ExecTypes as these are the only ExecTypes that we
     * have constants in the Pig codebase for.
     * 
     * @param execType
     * @return
     */
    private static ExecType getSingleton(ExecType execType) {
        if (execType instanceof MRExecType) {
            return ExecType.MAPREDUCE;
        }
        if (execType instanceof LocalExecType) {
            return ExecType.LOCAL;
        }
        // if it is not MR specific but rather a different
        // execution engine, we don't have access to any
        // constants that can act as singletons, so we just
        // use the given instance
        return execType;
    }

    public static ExecType fromString(String execType) throws PigException {
        Properties properties = PropertiesUtil.loadDefaultProperties();
        properties.setProperty("exectype", execType);
        return selectExecType(properties);
    }

}
