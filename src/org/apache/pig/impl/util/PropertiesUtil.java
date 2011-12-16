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

package org.apache.pig.impl.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PropertiesUtil {
    private static final String DEFAULT_PROPERTIES_FILE = "/pig-default.properties";
    private static final String PROPERTIES_FILE = "/pig.properties";
    private final static Log log = LogFactory.getLog(PropertiesUtil.class);

    /**
     * Loads the default properties from pig-default.properties and
     * pig.properties.
     * @param properties Properties object that needs to be loaded with the
     * properties' values.
     */
    public static void loadDefaultProperties(Properties properties) {
        loadPropertiesFromFile(properties,
                System.getProperty("user.home") + "/.pigrc");
        loadPropertiesFromClasspath(properties, DEFAULT_PROPERTIES_FILE);
        loadPropertiesFromClasspath(properties, PROPERTIES_FILE);
        setDefaultsIfUnset(properties);
        
        //Now set these as system properties only if they are not already defined.
        if (log.isDebugEnabled()) {
            for (Object o: properties.keySet()) {
                String propertyName = (String) o ;
                StringBuilder sb = new StringBuilder() ;
                sb.append("Found property ") ;
                sb.append(propertyName) ;
                sb.append("=") ;
                sb.append(properties.get(propertyName).toString()) ;
                log.debug(sb.toString()) ;
            }
        }

		// Add System properties which include command line overrides
		// Any existing keys will be overridden
		properties.putAll(System.getProperties());

		// For telling error fast when there are problems
		ConfigurationValidator.validatePigProperties(properties) ;
    }

    /**
     * Loads the properties from a given file.
     * @param properties Properties object that is to be loaded.
     * @param fileName file name of the file that contains the properties.
     */
    public static void loadPropertiesFromFile(Properties properties,
            String fileName) {
        BufferedInputStream bis = null;
        Properties pigrcProps = new Properties() ;
        try {
            File pigrcFile = new File(fileName);
            if (pigrcFile.exists()) {
                if (fileName.endsWith("/.pigrc")) {
                    log.warn(pigrcFile.getAbsolutePath()
                            + " exists but will be deprecated soon." +
                            		" Use conf/pig.properties instead!");
                }

                bis = new BufferedInputStream(new FileInputStream(pigrcFile));
                pigrcProps.load(bis) ;
            }
        } catch (Exception e) {
            log.error("unable to parse .pigrc :", e);
        } finally {
            if (bis != null) try {bis.close();} catch (Exception e) {}
        }

		properties.putAll(pigrcProps);
    }

    /**
     * Finds the file with the given file name in the classpath and loads the
     * properties provided in it.
     * @param properties the properties object that needs to be loaded with the
     * property values provided in the file.
     * @param fileName file name of the properties' file.
     */
    private static void loadPropertiesFromClasspath(Properties properties,
            String fileName) {
        InputStream inputStream = null;
        Class<PropertiesUtil> clazz = PropertiesUtil.class;
        try {
            inputStream = clazz
                    .getResourceAsStream(fileName);
            if (inputStream == null) {
                String msg = "no " + fileName +
                " configuration file available in the classpath";
                log.debug(msg);
            } else {
                properties.load(inputStream);
            }
        } catch (Exception e) {
            log.error("unable to parse " + fileName + " :", e);
        } finally {
            if (inputStream != null) try {inputStream.close();} catch (Exception e) {}
        }
    }

    /**
     * Sets properties to their default values if not set by Client
     * @param properties
     */
    private static void setDefaultsIfUnset(Properties properties) {
    	if (properties.getProperty("aggregate.warning") == null) {
            //by default warning aggregation is on
            properties.setProperty("aggregate.warning", ""+true);
        }

        if (properties.getProperty("opt.multiquery") == null) {
            //by default multiquery optimization is on
            properties.setProperty("opt.multiquery", ""+true);
        }

        if (properties.getProperty("stop.on.failure") == null) {
            //by default we keep going on error on the backend
            properties.setProperty("stop.on.failure", ""+false);
        }
    }
    
    /**
     * Loads default properties.
     * @return default properties
     */
    public static Properties loadDefaultProperties() {
        Properties properties = new Properties();
        loadDefaultProperties(properties);
        return properties;
    }

}
