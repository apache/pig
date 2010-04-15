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

import java.util.HashMap;
import java.util.Map;

public class OwlConfig {

    private static final String LOG_FILE = "log/logfile";
    private static final String LOG_LAYOUT = "log/layout";

    private static final String LOG_LEVELS = "log/levels";

    private static final String JDBC_DRIVER = "jdbc/driver";
    private static final String JDBC_URL = "jdbc/url";
    private static final String JDBC_USER = "jdbc/user";
    private static final String JDBC_PASSWD = "jdbc/password";

    public final static String PREFS_NODE_NAME = "org/apache/hadoop/owl";

    private static Config cfg = null;

    private static void instantiateConfig(){
        // cfg = new PreferencesBasedConfigImpl();
        cfg = new XMLConfigurationBasedConfigImpl();
    }

    @Deprecated
    public static void setConfigParam(String key, String value) throws OwlException {
        if (cfg == null){
            instantiateConfig();
        }
        cfg.setConfigParam(PREFS_NODE_NAME,key,value);
    }

    public static String getConfigParam(String key) throws OwlException {
        if (cfg == null){
            instantiateConfig();
        }
        return cfg.getConfigParam(PREFS_NODE_NAME,key);
    }

    public static String getJdbcDriver() throws OwlException {
        return getConfigParam(JDBC_DRIVER);
    }

    public static String getJdbcUrl() throws OwlException {
        return getConfigParam(JDBC_URL);
    }

    public static String getJdbcUser() throws OwlException {
        return getConfigParam(JDBC_USER);
    }

    public static String getJdbcPassword() throws OwlException {
        return getConfigParam(JDBC_PASSWD);
    }

    public static Map<String, String> getDbConnectivityProperties() {
        Map<String,String> props = null;

        // get necessary parameters, without which we return null
        try {
            String driver = getJdbcDriver();
            String url = getJdbcUrl();

            props = new HashMap<String,String>();
            props.put("javax.persistence.jdbc.driver", driver);
            props.put("javax.persistence.jdbc.url", url);
        } catch (OwlException e) {
            return null;
        }

        // get other optional parameters
        try {
            String user = getJdbcUser();
            String passwd = getJdbcPassword();
            props.put("javax.persistence.jdbc.user", user);
            props.put("javax.persistence.jdbc.password", passwd);
        } catch (OwlException e1) {
            // do nothing, ignore.
        }

        return props;
    }

    public static String getLogFile() throws OwlException{
        return getConfigParam(LOG_FILE);
    }

    public static String getLogLayout() throws OwlException {
        return getConfigParam(LOG_LAYOUT);
    }

    public static String getLogLevels() throws OwlException {
        return getConfigParam(LOG_LEVELS);
    }


}
