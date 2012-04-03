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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.impl.PigContext;


public class JarManager {

    private static Log log = LogFactory.getLog(JarManager.class);
    /**
     * A container class to track the Jar files that need to be merged together to submit to Hadoop.
     */
    private static class JarListEntry {
        /**
         * The name of the Jar file to merge in.
         */
        String jar;
        /**
         * If this field is not null, only entries that start with this prefix will be merged in.
         */
        String prefix;

        JarListEntry(String jar, String prefix) {
            this.jar = jar;
            this.prefix = prefix;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof JarListEntry))
                return false;
            JarListEntry other = (JarListEntry) obj;
            if (!jar.equals(other.jar))
                return false;
            if (prefix == null)
                return other.prefix == null;
            return prefix.equals(other.prefix);
        }

        @Override
        public int hashCode() {
            return jar.hashCode() + (prefix == null ? 1 : prefix.hashCode());
        }
    }

    final static String pigPackagesToSend[] = { "org/apache/pig","org/apache/tools/bzip2r",
        "dk/brics/automaton", "org/antlr/runtime", "com/google/common", "org/codehaus/jackson" };
    
    /**
     * Create a jarfile in a temporary path, that is a merge of all the jarfiles containing the
     * functions and the core pig classes.
     * 
     * @param funcs
     *            the functions that will be used in a job and whose jar files need to be included
     *            in the final merged jar file.
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    public static void createJar(OutputStream os, Set<String> funcs, PigContext pigContext) throws ClassNotFoundException, IOException {
        Vector<JarListEntry> jarList = new Vector<JarListEntry>();
        for(String toSend: pigPackagesToSend) {
            addContainingJar(jarList, PigMapReduce.class, toSend, pigContext);
        }
        
        for (String func: funcs) {
            Class clazz = pigContext.getClassForAlias(func);
            if (clazz != null) {
                addContainingJar(jarList, clazz, null, pigContext);
            }
        }
        HashMap<String, String> contents = new HashMap<String, String>();
        JarOutputStream jarFile = new JarOutputStream(os);
        Iterator<JarListEntry> it = jarList.iterator();
        while (it.hasNext()) {
            JarListEntry jarEntry = it.next();
            // log.error("Adding " + jarEntry.jar + ":" + jarEntry.prefix);
            mergeJar(jarFile, jarEntry.jar, jarEntry.prefix, contents);
        }
        for (String scriptJar: pigContext.scriptJars) {
            mergeJar(jarFile, scriptJar, null, contents);
        }
        for (String path: pigContext.scriptFiles) {
            log.debug("Adding entry " + path + " to job jar" );
            InputStream stream = null;
            if (new File(path).exists()) {
                stream = new FileInputStream(new File(path));
            } else {
                stream = PigContext.getClassLoader().getResourceAsStream(path);
            }
            if (stream==null) {
                throw new IOException("Cannot find " + path);
            }
        	addStream(jarFile, path, stream, contents);
        }
        for (Map.Entry<String, File> entry : pigContext.getScriptFiles().entrySet()) {
            log.debug("Adding entry " + entry.getKey() + " to job jar" );
            InputStream stream = null;
            if (entry.getValue().exists()) {
                stream = new FileInputStream(entry.getValue());
            } else {
                stream = PigContext.getClassLoader().getResourceAsStream(entry.getValue().getPath());
            }
            if (stream==null) {
                throw new IOException("Cannot find " + entry.getValue().getPath());
            }
        	addStream(jarFile, entry.getKey(), stream, contents);
        }

        log.debug("Adding entry pigContext to job jar" );
        jarFile.putNextEntry(new ZipEntry("pigContext"));
        new ObjectOutputStream(jarFile).writeObject(pigContext);
        jarFile.close();
    }

    /**
     * Creates a Classloader based on the passed jarFile and any extra jar files.
     * 
     * @param jarFile
     *            the jar file to be part of the newly created Classloader. This jar file plus any
     *            jars in the extraJars list will constitute the classpath.
     * @return the new Classloader.
     * @throws MalformedURLException
     */
    static ClassLoader createCl(String jarFile, PigContext pigContext) throws MalformedURLException {
        int len = pigContext.extraJars.size();
        int passedJar = jarFile == null ? 0 : 1;
        URL urls[] = new URL[len + passedJar];
        if (jarFile != null) {
            urls[0] = new URL("file:" + jarFile);
        }
        for (int i = 0; i < pigContext.extraJars.size(); i++) {
            urls[i + passedJar] = new URL("file:" + pigContext.extraJars.get(i));
        }
        return new URLClassLoader(urls, PigMapReduce.class.getClassLoader());
    }
    

    /**
     * Merge one Jar file into another.
     * 
     * @param jarFile
     *            the stream of the target jar file.
     * @param jar
     *            the name of the jar file to be merged.
     * @param prefix
     *            if not null, only entries in jar that start with this prefix will be merged.
     * @param contents
     *            the current contents of jarFile. (Use to prevent duplicate entries.)
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void mergeJar(JarOutputStream jarFile, String jar, String prefix, Map<String, String> contents)
            throws FileNotFoundException, IOException {
        JarInputStream jarInput = new JarInputStream(new FileInputStream(jar));
        log.debug("Adding jar " + jar + (prefix != null ? " for prefix "+prefix : "" ) + " to job jar" );
        mergeJar(jarFile, jarInput, prefix, contents);
    }
    
    private static void mergeJar(JarOutputStream jarFile, URL jar, String prefix, Map<String, String> contents)
    throws FileNotFoundException, IOException {
        JarInputStream jarInput = new JarInputStream(jar.openStream());

        mergeJar(jarFile, jarInput, prefix, contents);
    }

    private static void mergeJar(JarOutputStream jarFile, JarInputStream jarInput, String prefix, Map<String, String> contents)
    throws FileNotFoundException, IOException {
        JarEntry entry;
        while ((entry = jarInput.getNextJarEntry()) != null) {
            if (prefix != null && !entry.getName().startsWith(prefix)) {
                continue;
            }
            addStream(jarFile, entry.getName(), jarInput, contents);
        }
    }
        /**
     * Adds a stream to a Jar file.
     * 
     * @param os
     *            the OutputStream of the Jar file to which the stream will be added.
     * @param name
     *            the name of the stream.
     * @param is
     *            the stream to add.
     * @param contents
     *            the current contents of the Jar file. (We use this to avoid adding two streams
     *            with the same name.
     * @throws IOException
     */
    private static void addStream(JarOutputStream os, String name, InputStream is, Map<String, String> contents)
            throws IOException {
        if (contents.get(name) != null) {
            return;
        }
        contents.put(name, "");
        os.putNextEntry(new JarEntry(name));
        byte buffer[] = new byte[4096];
        int rc;
        while ((rc = is.read(buffer)) > 0) {
            os.write(buffer, 0, rc);
        }
    }


    
    /**
     * Adds the Jar file containing the given class to the list of jar files to be merged.
     * 
     * @param jarList
     *            the list of jar files to be merged.
     * @param clazz
     *            the class in a jar file to be merged.
     * @param prefix
     *            if not null, only resources from the jar file that start with this prefix will be
     *            merged.
     */
    private static void addContainingJar(Vector<JarListEntry> jarList, Class clazz, String prefix, PigContext pigContext) {
        String jar = findContainingJar(clazz);
        if (pigContext.skipJars.contains(jar) && prefix == null)
            return;
        if (jar == null)
        {
            //throw new RuntimeException("Couldn't find the jar for " + clazz.getName());
            log.warn("Couldn't find the jar for " + clazz.getName() + ", skip it");
            return;
        }
        JarListEntry jarListEntry = new JarListEntry(jar, prefix);
        if (!jarList.contains(jarListEntry))
            jarList.add(jarListEntry);
    }


    /**
     * Find a jar that contains a class of the same name, if any. It will return a jar file, even if
     * that is not the first thing on the class path that has a class with the same name.
     * 
     * @param my_class
     *            the class to find
     * @return a jar file that contains the class, or null
     * @throws IOException
     */
    public static String findContainingJar(Class my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements();) {
                URL url = (URL) itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    // URLDecoder is a misnamed class, since it actually decodes
                    // x-www-form-urlencoded MIME type rather than actual
                    // URL encoding (which the file path has). Therefore it would
                    // decode +s to ' 's which is incorrect (spaces are actually
                    // either unencoded or encoded as "%20"). Replace +s first, so
                    // that they are kept sacred during the decoding process.
                    toReturn = toReturn.replaceAll("\\+", "%2B");
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
