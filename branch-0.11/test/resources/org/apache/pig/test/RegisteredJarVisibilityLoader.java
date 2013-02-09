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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.Properties;

/**
 * Please see {@link TestRegisteredJarVisibility} for information about this class.
 */
public class RegisteredJarVisibilityLoader extends PigStorage {
    private static final Log LOG = LogFactory.getLog(RegisteredJarVisibilityLoader.class);
    private static final String REGISTERED_JAR_VISIBILITY_SCHEMA = "registered.jar.visibility.schema";

    public RegisteredJarVisibilityLoader() throws IOException, ClassNotFoundException {
        // make sure classes that were visible here and through current
        // PigContext are the same
        Class<?> clazz = ClassLoaderSanityCheck.class;
        Class<?> loadedClass = Class.forName(clazz.getName(), true, PigContext.getClassLoader());
        if (!clazz.equals(loadedClass)) {
            throw new RuntimeException("class loader sanity check failed. "
                    + "please make sure jar registration does not result in "
                    + "multiple instances of same classes");
        }
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        UDFContext udfContext = UDFContext.getUDFContext();
        Properties properties = udfContext.getUDFProperties(RegisteredJarVisibilityLoader.class);

        if (!properties.containsKey(REGISTERED_JAR_VISIBILITY_SCHEMA)) {
            LOG.info("Storing " + RegisteredJarVisibilitySchema.class.getName() + " in UDFContext.");
            properties.put(REGISTERED_JAR_VISIBILITY_SCHEMA, new RegisteredJarVisibilitySchema());
            LOG.info("Stored " + RegisteredJarVisibilitySchema.class.getName() + " in UDFContext.");
        } else {
            LOG.info("Retrieving " + REGISTERED_JAR_VISIBILITY_SCHEMA + " from UDFContext.");
            RegisteredJarVisibilitySchema registeredJarVisibilitySchema =
                    (RegisteredJarVisibilitySchema) properties.get(REGISTERED_JAR_VISIBILITY_SCHEMA);
            LOG.info("Retrieved " + REGISTERED_JAR_VISIBILITY_SCHEMA + " from UDFContext.");
        }

        super.setLocation(location, job);
    }
}
