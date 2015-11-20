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
package org.apache.pig.parser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.pig.PigServer;
import org.apache.pig.tools.DownloadResolver;

public class RegisterResolver {

    private PigServer pigServer;

    public RegisterResolver(PigServer pigServer) {
	this.pigServer = pigServer;
    }

    /**
     * @param path
     * @param scriptingLang
     * @param namespace
     * @throws IOException
     */
    public void parseRegister(String path, String scriptingLang, String namespace) throws IOException {
        try {
            URI uri = new URI(path);
            if (resolvesToJar(uri)) {
                if (scriptingLang != null || namespace != null) {
                    throw new ParserException("Cannot register a jar with a scripting language or namespace");
                }
                URI[] uriList = resolve(uri);
                for (URI jarUri : uriList) {
                    pigServer.registerJar(jarUri.toString());
                }
            } else {
                pigServer.registerCode(path, scriptingLang, namespace);
            }
        } catch (URISyntaxException e) {
            throw new ParserException("URI " + path + " is incorrect.", e);
        }
    }

    /**
     * @param uri
     * @return List of URIs
     * @throws IOException
     */
    public URI[] resolve(URI uri) throws IOException {
        String scheme = uri.getScheme();
        if (scheme != null) {
            scheme = scheme.toLowerCase();
        }
        if (scheme == null || scheme.equals("file") || scheme.equals("hdfs")) {
            return new URI[] { uri };
        } else if (scheme.equals("ivy")) {
            DownloadResolver downloadResolver = DownloadResolver.getInstance();
            return downloadResolver.downloadArtifact(uri, pigServer);
        } else {
            throw new ParserException("Invalid Scheme: " + uri.getScheme());
        }
    }

    /**
     * @param uri
     * @return True if the uri is a jar or an ivy coordinate
     */
    private boolean resolvesToJar(URI uri) {
        String scheme = uri.getScheme();
	return (uri.toString().endsWith("jar") || scheme != null && scheme.toLowerCase().equals("ivy"));
    }

}
