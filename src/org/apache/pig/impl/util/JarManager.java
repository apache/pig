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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.antlr.runtime.CommonTokenStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;
import org.apache.tools.bzip2r.BZip2Constants;
import org.joda.time.DateTime;

import com.google.common.collect.Multimaps;

import dk.brics.automaton.Automaton;

public class JarManager {

    private static Log log = LogFactory.getLog(JarManager.class);

    private static enum DefaultPigPackages {

        PIG(PigMapReduce.class),
        BZIP2R(BZip2Constants.class),
        AUTOMATON(Automaton.class),
        ANTLR(CommonTokenStream.class),
        GUAVA(Multimaps.class),
        JODATIME(DateTime.class);

        private final Class pkgClass;

        DefaultPigPackages(Class pkgClass) {
            this.pkgClass = pkgClass;
        }

        public Class getPkgClass() {
            return pkgClass;
        }
    }

    public static File createPigScriptUDFJar(PigContext pigContext) throws IOException {
        File scriptUDFJarFile = File.createTempFile("PigScriptUDF", ".jar");
        // ensure the scriptUDFJarFile is deleted on exit
        scriptUDFJarFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(scriptUDFJarFile);
        HashMap<String, String> contents = new HashMap<String, String>();
        createPigScriptUDFJar(fos, pigContext, contents);

        if (!contents.isEmpty()) {
            FileInputStream fis = null;
            String md5 = null;
            try {
                fis = new FileInputStream(scriptUDFJarFile);
                md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
            File newScriptUDFJarFile = new File(scriptUDFJarFile.getParent(), "PigScriptUDF-" + md5 + ".jar");
            scriptUDFJarFile.renameTo(newScriptUDFJarFile);
            return newScriptUDFJarFile;
        }
        return null;
    }

    private static void createPigScriptUDFJar(OutputStream os, PigContext pigContext, HashMap<String, String> contents) throws IOException {
        JarOutputStream jarOutputStream = new JarOutputStream(os);
        for (String path: pigContext.scriptFiles) {
            log.debug("Adding entry " + path + " to job jar" );
            InputStream stream = null;
            File inputFile = new File(path);
            if (inputFile.exists()) {
                stream = new FileInputStream(inputFile);
            } else {
                stream = PigContext.getClassLoader().getResourceAsStream(path);
            }
            if (stream==null) {
                throw new IOException("Cannot find " + path);
            }
            try {
                addStream(jarOutputStream, path, stream, contents, inputFile.lastModified());
            } finally {
                stream.close();
            }
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
            try {
                addStream(jarOutputStream, entry.getKey(), stream, contents, entry.getValue().lastModified());
            } finally {
                stream.close();
            }
        }
        if (!contents.isEmpty()) {
            jarOutputStream.close();
        } else {
            os.close();
        }
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
     * @param timestamp
     *            timestamp of the entry
     * @throws IOException
     */
    private static void addStream(JarOutputStream os, String name, InputStream is, Map<String, String> contents,
            long timestamp)
            throws IOException {
        if (contents.get(name) != null) {
            return;
        }
        contents.put(name, "");
        JarEntry entry = new JarEntry(name);
        entry.setTime(timestamp);
        os.putNextEntry(entry);
        byte buffer[] = new byte[4096];
        int rc;
        while ((rc = is.read(buffer)) > 0) {
            os.write(buffer, 0, rc);
        }
    }

    public static List<String> getDefaultJars() {
        List<String> defaultJars = new ArrayList<String>();
        for (DefaultPigPackages pkgToSend : DefaultPigPackages.values()) {
            if(pkgToSend.equals(DefaultPigPackages.GUAVA) && HadoopShims.isHadoopYARN()) {
                continue; //Skip
            }
            String jar = findContainingJar(pkgToSend.getPkgClass());
            if (!defaultJars.contains(jar)) {
                defaultJars.add(jar);
            }
        }
        return defaultJars;
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
