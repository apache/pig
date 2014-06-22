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
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.antlr.runtime.CommonTokenStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.StreamingUDF;
import org.apache.tools.bzip2r.BZip2Constants;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.annotate.JacksonStdImpl;
import org.joda.time.DateTime;

import com.google.common.collect.Multimaps;

import dk.brics.automaton.Automaton;

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

    private static enum DefaultPigPackages {

        PIG("org/apache/pig", PigMapReduce.class),
        BZIP2R("org/apache/tools/bzip2r", BZip2Constants.class),
        AUTOMATON("dk/brics/automaton", Automaton.class),
        ANTLR("org/antlr/runtime", CommonTokenStream.class),
        GUAVA("com/google/common", Multimaps.class),
        JACKSON_CORE("org/codehaus/jackson", JsonPropertyOrder.class),
        JACKSON_MAPPER("org/codehaus/jackson", JacksonStdImpl.class),
        JODATIME("org/joda/time", DateTime.class);

        private final String pkgPrefix;
        private final Class pkgClass;

        DefaultPigPackages(String pkgPrefix, Class pkgClass) {
            this.pkgPrefix = pkgPrefix;
            this.pkgClass = pkgClass;
        }

        public String getPkgPrefix() {
            return pkgPrefix;
        }

        public Class getPkgClass() {
            return pkgClass;
        }
    }
    
    public static void createBootStrapJar(OutputStream os, PigContext pigContext) throws IOException {
        JarOutputStream jarFile = new JarOutputStream(os);
        HashMap<String, String> contents = new HashMap<String, String>();
        Vector<JarListEntry> jarList = new Vector<JarListEntry>();

        for (DefaultPigPackages pkgToSend : DefaultPigPackages.values()) {
            addContainingJar(jarList, pkgToSend.getPkgClass(), pkgToSend.getPkgPrefix(), pigContext);
        }

        Iterator<JarListEntry> it = jarList.iterator();
        while (it.hasNext()) {
            JarListEntry jarEntry = it.next();
            mergeJar(jarFile, jarEntry.jar, jarEntry.prefix, contents);
        }

        // Just like in MR Pig, we'll add the script files to Job.jar. For Jython, MR Pig packages
        // all the dependencies in Job.jar. For JRuby, we need the resource pigudf.rb, which is in
        // the pig jar. JavaScript files could be packaged either in the Job.jar or as Tez local
        // resources; MR Pig adds them to the Job.jar so that's what we will do also. Groovy files
        // must be added as Tez local resources in the TezPlanContainer (in MR Pig Groovy UDF's
        // are actually broken since they cannot be found by the GroovyScriptEngine).
        for (Map.Entry<String, File> entry : pigContext.getScriptFiles().entrySet()) {
            InputStream stream = null;
            if (entry.getValue().exists()) {
                stream = new FileInputStream(entry.getValue());
            } else {
                stream = PigContext.getClassLoader().getResourceAsStream(entry.getValue().getPath());
            }
            if (stream == null) {
                throw new IOException("Cannot find " + entry.getValue().getPath());
            }
            addStream(jarFile, entry.getKey(), stream, contents);
        }

        jarFile.close();
    }

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
        JarOutputStream jarFile = new JarOutputStream(os);
        HashMap<String, String> contents = new HashMap<String, String>();
        Vector<JarListEntry> jarList = new Vector<JarListEntry>();

        for (DefaultPigPackages pkgToSend : DefaultPigPackages.values()) {
            addContainingJar(jarList, pkgToSend.getPkgClass(), pkgToSend.getPkgPrefix(), pigContext);
        }

        for (String func: funcs) {
            Class clazz = pigContext.getClassForAlias(func);
            if (clazz != null) {
                addContainingJar(jarList, clazz, null, pigContext);
                
                if (clazz.getSimpleName().equals("StreamingUDF")) {
                    for (String fileName : StreamingUDF.getResourcesForJar()) {
                        InputStream in = JarManager.class.getResourceAsStream(fileName);
                        addStream(jarFile, fileName.substring(1), in, contents);
                    }
                }
            }
        }

        Iterator<JarListEntry> it = jarList.iterator();
        while (it.hasNext()) {
            JarListEntry jarEntry = it.next();
            // log.error("Adding " + jarEntry.jar + ":" + jarEntry.prefix);
            mergeJar(jarFile, jarEntry.jar, jarEntry.prefix, contents);
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
        if (pigContext.predeployedJars.contains(jar))
            return;
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
        ClassLoader loader = PigContext.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
        try {
            Enumeration<URL> itr = null;
            //Try to find the class in registered jars
            if (loader instanceof URLClassLoader) {
                itr = ((URLClassLoader) loader).findResources(class_file);
            }
            //Try system classloader if not URLClassLoader or no resources found in URLClassLoader
            if (itr == null || !itr.hasMoreElements()) {
                itr = loader.getResources(class_file);
            }
            for (; itr.hasMoreElements();) {
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
    
    /**
     * Add the jars containing the given classes to the job's configuration
     * such that JobClient will ship them to the cluster and add them to
     * the DistributedCache
     * 
     * @param job
     *           Job object
     * @param classes
     *            classes to find
     * @throws IOException
     */
    public static void addDependencyJars(Job job, Class<?>... classes)
            throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Set<String> jars = new HashSet<String>();
        jars.addAll(conf.getStringCollection("tmpjars"));
        addQualifiedJarsName(fs, jars, classes);
        if (jars.isEmpty())
            return;
        conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
    }
    
    /**
     * Add the qualified path name of jars containing the given classes 
     * 
     * @param fs
     *            FileSystem object
     * @param jars
     *            the resolved path names to be added to this set
     * @param classes
     *            classes to find
     */
    private static void addQualifiedJarsName(FileSystem fs, Set<String> jars, Class<?>... classes) {
        URI fsUri = fs.getUri();
        Path workingDir = fs.getWorkingDirectory();
        for (Class<?> clazz : classes) {
            String jarName = findContainingJar(clazz);
            if (jarName == null) {
                log.warn("Could not find jar for class " + clazz);
                continue;
            }
            jars.add(new Path(jarName).makeQualified(fsUri, workingDir).toString());
        }
    }

}
