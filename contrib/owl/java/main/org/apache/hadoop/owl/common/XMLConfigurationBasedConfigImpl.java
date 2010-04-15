
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

package org.apache.hadoop.owl.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class XMLConfigurationBasedConfigImpl implements Config {

    private static final int RELOAD_INTERVAL = 300000; // 300k msec =  5 mins
    private static final String PROPERTIES_CONFIG_FILE = "org.apache.hadoop.owl.xmlconfig";
    // 1 minute reload interval for picking up new preferences
    private long timeToReload = 0;

    private XMLConfiguration cfg;

    private void loadConfig() throws OwlException{
        String configFile = System.getProperty(PROPERTIES_CONFIG_FILE);
        if (configFile != null){
            loadFromFile(configFile);
        }
        timeToReload = System.currentTimeMillis()+RELOAD_INTERVAL;
    }

    @Override
    public String getConfigParam(String nodeName, String key) throws OwlException {
        if ((cfg == null) || (System.currentTimeMillis() > timeToReload)){
            loadConfig();
        }
        String node = nodeName.replaceAll("/", ".") + "." + key.replaceAll("/", ".");
        if (cfg !=null){
            String value = cfg.getString(node);
            if (value != null){
                return value;
            }
        }
        throw new OwlException(ErrorType.ERROR_UNKNOWN_CONFIG_PARAMETER,node);
    }

    @Override
    public void loadFromFile(String fileName) throws OwlException {
        try {
            cfg = new XMLConfiguration(fileName);
        } catch (ConfigurationException e) {
            throw new OwlException(ErrorType.ERROR_UNABLE_TO_LOAD_CONFIG,e);
        }
    }

    @Override
    @Deprecated
    public void setConfigParam(String nodeName, String key, String value) throws OwlException {
        // Config needs only to be read-only from a daemon, and write-config implementations 
        // often have too many exception scenarios to fit the really simple model we want
        // So left unimplemented, deprecated method.
        throw new OwlException(ErrorType.ERROR_UNABLE_TO_STORE_CONFIG,"unimplemented for XMLConfigurationBasedConfigImpl");
    }

}
