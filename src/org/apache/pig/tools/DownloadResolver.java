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
package org.apache.pig.tools;

import groovy.grape.Grape;
import groovy.lang.GroovyClassLoader;

import java.io.File;
import java.net.URI;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.parser.ParserException;

public class DownloadResolver {

    private static final String IVY_FILE_NAME = "ivysettings.xml";
    private static final Log LOG = LogFactory.getLog(DownloadResolver.class);
    private static DownloadResolver downloadResolver = new DownloadResolver();

    private DownloadResolver() {
        if (System.getProperty("grape.config") != null) {
            LOG.info("Using ivysettings file from " + System.getProperty("grape.config"));
        } else {
            // Retrieve the ivysettings configuration file
            Map<String, String> envMap = System.getenv();
            File confFile = null;
            // Check for configuration file in PIG_CONF_DIR
            if (envMap.containsKey("PIG_CONF_DIR")) {
                confFile = new File(new File(envMap.get("PIG_CONF_DIR")).getPath(), IVY_FILE_NAME);
            }

            // Check for configuration file in PIG_HOME if not found in PIG_CONF_DIR
            if (confFile == null || !confFile.exists()) {
                confFile = new File(new File(envMap.get("PIG_HOME"), "conf").getPath(), IVY_FILE_NAME);
            }

            // Check for configuration file in Classloader if not found in PIG_CONF_DIR and PIG_HOME
            if (confFile == null || !confFile.exists()) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                if (classLoader.getResource(IVY_FILE_NAME) != null) {
                    LOG.info("Found ivysettings file in classpath");
                    confFile = new File(classLoader.getResource(IVY_FILE_NAME).getFile());

                    if (!confFile.exists()) {
                        // ivysettings file resides inside a jar
                        try {
                            List<String> ivyLines = IOUtils.readLines(classLoader.getResourceAsStream(IVY_FILE_NAME));
                            confFile = File.createTempFile("ivysettings", ".xml");
                            confFile.deleteOnExit();
                            for(String str: ivyLines) {
                                FileUtils.writeStringToFile(confFile, str, true);
                            }
                        } catch (Exception e) {
                            LOG.warn("Could not create an ivysettings file from resource");
                        }
                    }
                }
            }

            // Set the Configuration file
            if (confFile != null && confFile.exists()) {
                LOG.info("Using ivysettings file from " + confFile.toString());
                System.setProperty("grape.config", confFile.toString());
            } else {
                LOG.warn("Could not find custom ivysettings file in PIG_CONF_DIR or PIG_HOME or classpath.");
            }
        }
    }

    /**
     * @return Singleton Object
     */
    public static DownloadResolver getInstance() {
        return downloadResolver;
    }

    /**
     * @param uri
     * @return A map of all Query String Parameters
     */
    private Map<String, String> parseQueryString(URI uri) {
        Map<String, String> paramMap = new HashMap<String, String>();
        String queryString = uri.getQuery();
        if (queryString != null) {
            String queryParams[] = queryString.split("&");
            for (String param : queryParams) {
                String[] parts = param.split("=");
                if (parts.length == 2) {
                    String name = parts[0].toLowerCase();
                    String value = parts[1];
                    paramMap.put(name, value);
                }
            }
        }
        return paramMap;
    }

    /**
     * @param uri
     * @return Returns a Map containing the organization, module, version and
     *         all the query string parameters
     * @throws ParserException
     */
    private Map<String, Object> parseUri(URI uri) throws ParserException {
        // Parse uri for artifact organization, module and version
        Map<String, Object> uriMap = new HashMap<String, Object>();
        String authority = uri.getAuthority();
        if (authority != null) {
            String[] tokens = authority.split(":", -1);
            if (tokens.length == 3) {
                uriMap.put("org", tokens[0]);
                if (tokens[1].isEmpty()) {
                    throw new ParserException("Please specify the artifact module.");
                }
                uriMap.put("module", tokens[1]);
                uriMap.put("version", tokens[2]);
            } else {
                throw new ParserException("Invalid Artifact. Please specify organization, module and version");
            }
        } else {
            throw new ParserException("Invalid Artifact. Please specify organization, module and version");
        }

        // Parse query string for exclude list and other parameters
        uriMap.putAll(parseQueryString(uri));
        if (uriMap.containsKey("transitive")) {
            uriMap.put("transitive", Boolean.parseBoolean(uriMap.get("transitive").toString()));
        }
        List<Map<String, Object>> excludeList = new LinkedList<Map<String, Object>>();
        if (uriMap.containsKey("exclude")) {
            for (String exclude : uriMap.get("exclude").toString().split(",")) {
                Map<String, Object> excludeMap = new HashMap<String, Object>();
                String parts[] = exclude.split(":", -1);
                if (parts.length == 2) {
                    excludeMap.put("group", parts[0]);
                    excludeMap.put("module", parts[1]);
                } else {
                    throw new ParserException("Exclude must contain organization and module separated by a colon.");
                }
                excludeList.add(excludeMap);
            }
        }
        uriMap.put("excludes", excludeList);

        return uriMap;
    }

    /**
     * @param uri
     * @return List of URIs of the downloaded jars
     * @throws ParserException
     */
    public URI[] downloadArtifact(URI uri, PigServer pigServer) throws ParserException {
        Configuration conf = ConfigurationUtil.toConfiguration(pigServer.getPigContext().getProperties());

        String artDownloadLocation = conf.get(PigConfiguration.PIG_ARTIFACTS_DOWNLOAD_LOCATION);

        if (artDownloadLocation != null) {
            LOG.info("Artifacts will be downloaded to " + artDownloadLocation);
            System.setProperty("grape.root", artDownloadLocation);
        } else if (System.getProperty("grape.root") != null) {
            LOG.info("Artifacts will be downloaded to " + System.getProperty("grape.root"));
        } else {
            LOG.info("Artifacts will be downloaded to default location. Please check ~/.groovy/grapes");
        }

        Map<String, Object> uriMap = parseUri(uri);
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("classLoader", new GroovyClassLoader());
        args.put("excludes", uriMap.get("excludes"));
        return Grape.resolve(args, uriMap);
    }

}
