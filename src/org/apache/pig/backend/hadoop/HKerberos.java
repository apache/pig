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

package org.apache.pig.backend.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Support for logging in using a kerberos keytab file.
 *
 * <br/>
 * Kerberos is a authentication system that uses tickets with a limited valitity time.<br/>
 * As a consequence running a pig script on a kerberos secured hadoop cluster limits the running time to at most
 * the remaining validity time of these kerberos tickets. When doing really complex analytics this may become a
 * problem as the job may need to run for a longer time than these ticket times allow.<br/>
 * A kerberos keytab file is essentially a Kerberos specific form of the password of a user. <br/>
 * It is possible to enable a Hadoop job to request new tickets when they expire by creating a keytab file and
 * make it part of the job that is running in the cluster.
 * This will extend the maximum job duration beyond the maximum renew time of the kerberos tickets.<br/>
 * <br/>
 * Usage:
 * <ol>
 *      <li>Create a keytab file for the required principal.<br/>
 *      <p>Using the ktutil tool you can create a keytab using roughly these commands:<br/>
 *      <i>addent -password -p niels@EXAMPLE.NL -k 1 -e rc4-hmac<br/>
 *      addent -password -p niels@EXAMPLE.NL -k 1 -e aes256-cts<br/>
 *      wkt niels.keytab</i></p>
 *      </li>
 *      <li>Set the following properties (either via the .pigrc file or on the command line via -P file)<br/>
 *          <ul>
 *          <li><i>java.security.krb5.conf</i><br/>
 *              The path to the local krb5.conf file.<br/>
 *              Usually this is "/etc/krb5.conf"</li>
 *          <li><i>hadoop.security.krb5.principal</i><br/>
 *              The pricipal you want to login with.<br/>
 *              Usually this would look like this "niels@EXAMPLE.NL"</li>
 *          <li><i>hadoop.security.krb5.keytab</i><br/>
 *              The path to the local keytab file that must be used to authenticate with.<br/>
 *              Usually this would look like this "/home/niels/.krb/niels.keytab"</li>
 *          </ul></li>
 * </ol>
 * NOTE: All paths in these variables are local to the client system starting the actual pig script.
 * This can be run without any special access to the cluster nodes.
 */
public class HKerberos {
    private static final Log LOG = LogFactory.getLog(HKerberos.class);

    public static void tryKerberosKeytabLogin(Configuration conf) {
        // Before we can actually connect we may need to login using the provided credentials.
        if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation loginUser;
            try {
                loginUser = UserGroupInformation.getLoginUser();
            } catch (IOException e) {
                LOG.error("Unable to start attempt to login using Kerberos keytab: " + e.getMessage());
                return;
            }

            // If we are logged in into Kerberos with a keytab we can skip this to avoid needless logins
            if (!loginUser.hasKerberosCredentials() && !loginUser.isFromKeytab()) {
                String krb5Conf      = conf.get("java.security.krb5.conf");
                String krb5Principal = conf.get("hadoop.security.krb5.principal");
                String krb5Keytab    = conf.get("hadoop.security.krb5.keytab");

                // Only attempt login if we have all the required settings.
                if (krb5Conf != null && krb5Principal != null && krb5Keytab != null) {
                    LOG.info("Trying login using Kerberos Keytab");
                    LOG.info("krb5: Conf      = " + krb5Conf);
                    LOG.info("krb5: Principal = " + krb5Principal);
                    LOG.info("krb5: Keytab    = " + krb5Keytab);
                    System.setProperty("java.security.krb5.conf", krb5Conf);
                    try {
                        UserGroupInformation.loginUserFromKeytab(krb5Principal, krb5Keytab);
                    } catch (IOException e) {
                        LOG.error("Unable to perform keytab based kerberos authentication: " + e.getMessage());
                    }
                }
            }
        }
    }

}
