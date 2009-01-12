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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfigurationValidator {
    
    private final static Log log = LogFactory.getLog(PropertiesUtil.class);
    /***
     * All pig configurations should be validated in here before use
     * @param properties
     */
    
    public static void validatePigProperties(Properties properties) {
        ensureLongType(properties, "pig.spill.size.threshold", 5000000L) ;
        ensureLongType(properties, "pig.spill.gc.activation.size", 40000000L) ;   
    }
    
    /**
     * Validate properties which need to be validated and return *ONLY* those
     * @param properties The Properties object containing all PIG properties
     * @return The properties object containing *ONLY* properties which were validated
     * (Typically these are user editable properties and should match the properties
     * validated in ValidatePigProperties(Properties properties))
     */
    public static Properties getValidatedProperties(Properties properties) {
        Properties result = new Properties();
        String[] propertiesToValidate = { "pig.spill.size.threshold", "pig.spill.gc.activation.size" };
        
        // validate the incoming properties
        validatePigProperties(properties);
        
        // return only properties that we validated
        for (String p : propertiesToValidate) {
            result.setProperty(p, properties.getProperty(p));
        }        
        return result;        
    }
    
    private static void ensureLongType(Properties properties,
                                       String key, 
                                       long defaultValue) {
        String str = properties.getProperty(key) ;          
        if (str != null) {
            try {
                Long.parseLong(str) ;
            }
            catch (NumberFormatException  nfe) {
                log.error(str + " has to be parsable to long") ;
                properties.setProperty(key, Long.toString(defaultValue)) ;
            }
        }
        else {
            properties.setProperty(key, Long.toString(defaultValue)) ;
        }        
    }
}
