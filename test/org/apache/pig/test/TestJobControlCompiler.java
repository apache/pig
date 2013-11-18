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
package org.apache.pig.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJobControlCompiler {

    private static final Configuration CONF = new Configuration();

    
    @BeforeClass
    public static void setupClass() throws Exception {
        // creating a hadoop-site.xml and making it visible to Pig
        // making sure it is at the same location as for other tests to not pick
        // up a conf from a previous test
        File conf_dir = new File("build/classes");
        File hadoopSite = new File(conf_dir, "hadoop-site.xml");
        hadoopSite.deleteOnExit();
        FileWriter fw = new FileWriter(hadoopSite);
        try {
            fw.write("<?xml version=\"1.0\"?>\n");
            fw.write("<?xml-stylesheet type=\"text/xsl\" href=\"nutch-conf.xsl\"?>\n");
            fw.write("<configuration>\n");
            fw.write("</configuration>\n");
        } finally {
            fw.close();
        }
        // making hadoop-site.xml visible to Pig as it REQUIRES!!! one when
        // running in mapred mode
        Thread.currentThread().setContextClassLoader(
                new URLClassLoader(new URL[] { conf_dir.toURI().toURL() }));
    }
  /**
   * specifically tests that REGISTERED jars get added to distributed cache instead of merged into 
   * the job jar
   * @throws Exception
   */
  @Test
  public void testJarAddedToDistributedCache() throws Exception {

    // creating a jar with a UDF *not* in the current classloader
    File tmpFile = File.createTempFile("Some_", ".jar");
    tmpFile.deleteOnExit();
    String className = createTestJar(tmpFile);
    final String testUDFFileName = className+".class";

    // JobControlCompiler setup
    PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
    PigContext pigContext = pigServer.getPigContext();
    pigContext.connect();
    pigContext.addJar(tmpFile.getAbsolutePath());
    JobControlCompiler jobControlCompiler = new JobControlCompiler(pigContext, CONF);
    MROperPlan plan = new MROperPlan();
    MapReduceOper mro = new MapReduceOper(new OperatorKey());
    mro.UDFs = new HashSet<String>();
    mro.UDFs.add(className+"()");
    plan.add(mro);

    // compiling the job
    JobControl jobControl = jobControlCompiler.compile(plan , "test");
    JobConf jobConf = jobControl.getWaitingJobs().get(0).getJobConf();

    // verifying the jar gets on distributed cache
    Path[] fileClassPaths = DistributedCache.getFileClassPaths(jobConf);
    Assert.assertEquals("size 1 for "+Arrays.toString(fileClassPaths), 1, fileClassPaths.length);
    Path distributedCachePath = fileClassPaths[0];
    Assert.assertEquals("ends with jar name: "+distributedCachePath, distributedCachePath.getName(), tmpFile.getName());
    // hadoop bug requires path to not contain hdfs://hotname in front
    Assert.assertTrue("starts with /: "+distributedCachePath, 
        distributedCachePath.toString().startsWith("/"));
    Assert.assertTrue("jar pushed to distributed cache should contain testUDF", 
        jarContainsFileNamed(new File(fileClassPaths[0].toUri().getPath()), testUDFFileName));

    // verifying the job jar does not contain the UDF
    File submitJarFile = new File(jobConf.get("mapred.jar"));
    Assert.assertFalse("the mapred.jar should *not* contain the testUDF", jarContainsFileNamed(submitJarFile, testUDFFileName));

  }

    private static List<File> createFiles(String... extensions)
            throws IOException {
        List<File> files = new ArrayList<File>();
        for (String extension : extensions) {
            File file = File.createTempFile("file", extension);
            file.deleteOnExit();
            files.add(file);
        }
        return files;
    }

    private static void assertFilesInDistributedCache(URI[] uris, int size,
            String... extensions) {
        Assert.assertEquals(size, uris.length);
        for (int i = 0; i < uris.length; i++) {
            Assert.assertTrue(uris[i].toString().endsWith(extensions[i]));
        }
    }

    @Test
    public void testAddArchiveToDistributedCache() throws IOException {
        final File textFile = File.createTempFile("file", ".txt");
        textFile.deleteOnExit();

        final List<File> zipArchives = createFiles(".zip");
        zipArchives.add(textFile);
        final List<File> tarArchives = createFiles(".tgz", ".tar.gz", ".tar");

        final PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
        final PigContext pigContext = pigServer.getPigContext();
        pigContext.connect();
        pigContext.getProperties().put("pig.streaming.ship.files",
                StringUtils.join(zipArchives, ","));
        pigContext.getProperties().put("pig.streaming.cache.files",
                StringUtils.join(tarArchives, ","));
        final JobControlCompiler jobControlCompiler = new JobControlCompiler(
                pigContext, CONF);

        final MROperPlan plan = new MROperPlan();
        plan.add(new MapReduceOper(new OperatorKey()));

        final JobControl jobControl = jobControlCompiler.compile(plan, "test");
        final JobConf jobConf = jobControl.getWaitingJobs().get(0).getJobConf();
        
        assertFilesInDistributedCache(DistributedCache.getCacheFiles(jobConf),
                1, ".txt");
        assertFilesInDistributedCache(
                DistributedCache.getCacheArchives(jobConf), 4, ".zip", ".tgz",
                ".tar.gz", ".tar");
    }

    @Test
    public void testEstimateNumberOfReducers() throws Exception {
        Assert.assertEquals(2, JobControlCompiler.estimateNumberOfReducers(
            new Job(CONF), createMockPOLoadMapReduceOper(2L * 1000 * 1000 * 999)));

        Assert.assertEquals(2, JobControlCompiler.estimateNumberOfReducers(
            new Job(CONF), createMockPOLoadMapReduceOper(2L * 1000 * 1000 * 1000)));

        Assert.assertEquals(3, JobControlCompiler.estimateNumberOfReducers(
            new Job(CONF), createMockPOLoadMapReduceOper(2L * 1000 * 1000 * 1001)));
    }

    private static MapReduceOper createMockPOLoadMapReduceOper(long size) throws Exception {
        MapReduceOper mro = new MapReduceOper(new OperatorKey());
        mro.mapPlan.add(createPOLoadWithSize(size, new PigStorage()));
        return mro;
    }

    public static POLoad createPOLoadWithSize(long size, LoadFunc loadFunc) throws Exception {
        File file = File.createTempFile("tempFile", ".tmp");
        file.deleteOnExit();
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.setLength(size);
        f.close();

        loadFunc.setLocation(file.getAbsolutePath(), new org.apache.hadoop.mapreduce.Job(CONF));
        FuncSpec funcSpec = new FuncSpec(loadFunc.getClass().getCanonicalName());
        POLoad poLoad = new POLoad(new OperatorKey(), loadFunc);
        poLoad.setLFile(new FileSpec(file.getAbsolutePath(), funcSpec));
        poLoad.setPc(new PigContext());
        poLoad.setUp();

        return poLoad;
    }

  /**
   * checks if the given file name is in the jar 
   * @param jarFile the jar to check
   * @param name the name to find (full path in the jar)
   * @return true if the name was found
   * @throws IOException
   */
  private boolean jarContainsFileNamed(File jarFile, String name) throws IOException {
    Enumeration<JarEntry> entries = new JarFile(jarFile).entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      if (entry.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * creates a jar containing a UDF not in the current classloader
   * @param jarFile the jar to create
   * @return the name of the class created (in the default package)
   * @throws IOException
   * @throws FileNotFoundException
   */
  private String createTestJar(File jarFile) throws IOException, FileNotFoundException {

    // creating the source .java file
    File javaFile = File.createTempFile("TestUDF", ".java");
    javaFile.deleteOnExit();
    String className = javaFile.getName().substring(0, javaFile.getName().lastIndexOf('.'));
    FileWriter fw = new FileWriter(javaFile);
    try {
      fw.write("import org.apache.pig.EvalFunc;\n");
      fw.write("import org.apache.pig.data.Tuple;\n");
      fw.write("import java.io.IOException;\n");
      fw.write("public class "+className+" extends EvalFunc<String> {\n");
      fw.write("  public String exec(Tuple input) throws IOException {\n");
      fw.write("    return \"test\";\n");
      fw.write("  }\n");
      fw.write("}\n");
    } finally {
      fw.close();
    }

    // compiling it
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
    Iterable<? extends JavaFileObject> compilationUnits1 = fileManager.getJavaFileObjects(javaFile);
    CompilationTask task = compiler.getTask(null, fileManager, null, null, null, compilationUnits1);
    task.call();

    // here is the compiled file
    File classFile = new File(javaFile.getParentFile(), className+".class");
    Assert.assertTrue(classFile.exists());

    // putting it in the jar
    JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile));
    try {
      jos.putNextEntry(new ZipEntry(classFile.getName()));
      try {
        InputStream testClassContentIS = new FileInputStream(classFile);
        try {
          byte[] buffer = new byte[64000];
          int n;
          while ((n = testClassContentIS.read(buffer)) != -1) {
            jos.write(buffer, 0, n);
          }
        } finally {
          testClassContentIS.close();
        }
      }finally {
        jos.closeEntry();
      }
    } finally {
      jos.close();
    }

    return className;
  }
}
