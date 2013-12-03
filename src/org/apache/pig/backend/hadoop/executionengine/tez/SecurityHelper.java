/**
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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.pig.classification.InterfaceAudience;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Charsets;

/**
 * This class duplicates some security related private methods from
 * org.apache.hadoop.mapreduce.JobSubmitter for Tez.
 */
@InterfaceAudience.Private
public class SecurityHelper {

    private static Log LOG = LogFactory.getLog(SecurityHelper.class);

    private SecurityHelper() {
    }

    @SuppressWarnings("unchecked")
    private static void readTokensFromFiles(Configuration conf,
            Credentials credentials) throws IOException {
        // add tokens and secrets coming from a token storage file
        String binaryTokenFilename = conf
                .get("mapreduce.job.credentials.binary");
        if (binaryTokenFilename != null) {
            Credentials binary = Credentials.readTokenStorageFile(new Path(
                    "file:///" + binaryTokenFilename), conf);
            credentials.addAll(binary);
        }
        // add secret keys coming from a json file
        String tokensFileName = conf.get("mapreduce.job.credentials.json");
        if (tokensFileName != null) {
            LOG.info("loading user's secret keys from " + tokensFileName);
            String localFileName = new Path(tokensFileName).toUri().getPath();

            boolean json_error = false;
            try {
                // read JSON
                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> nm = mapper.readValue(new File(
                        localFileName), Map.class);

                for (Map.Entry<String, String> ent : nm.entrySet()) {
                    credentials.addSecretKey(new Text(ent.getKey()), ent
                            .getValue().getBytes(Charsets.UTF_8));
                }
            } catch (JsonMappingException e) {
                json_error = true;
            } catch (JsonParseException e) {
                json_error = true;
            }
            if (json_error)
                LOG.warn("couldn't parse Token Cache JSON file with user secret keys");
        }
    }

    // get secret keys and tokens and store them into TokenCache
    public static void populateTokenCache(Configuration conf,
            Credentials credentials) throws IOException {
        readTokensFromFiles(conf, credentials);
        // add the delegation tokens from configuration
        String[] nameNodes = conf.getStrings(MRJobConfig.JOB_NAMENODES);
        LOG.debug("adding the following namenodes' delegation tokens:"
                + Arrays.toString(nameNodes));
        if (nameNodes != null) {
            Path[] ps = new Path[nameNodes.length];
            for (int i = 0; i < nameNodes.length; i++) {
                ps[i] = new Path(nameNodes[i]);
            }
            TokenCache.obtainTokensForNamenodes(credentials, ps, conf);
        }
    }

}
