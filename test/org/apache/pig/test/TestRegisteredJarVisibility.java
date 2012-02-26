package org.apache.pig.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensure classes from a registered jar are available in the UDFContext.
 * Please see PIG-2532 for additional details.
 */
public class TestRegisteredJarVisibility {
    private static final Log LOG = LogFactory.getLog(TestRegisteredJarVisibility.class);
    private static final String JAR_FILE_NAME = "test-foo-loader.jar";
    private static final String PACKAGE_NAME = "org.apache.pig.test";
    // Actual data is not important. Reusing an existing input file.
    private static final File INPUT_FILE = new File("test/data/pigunit/top_queries_input_data.txt");

    private MiniCluster cluster;
    private File jarFile;

    @Before()
    public void setUp() throws IOException {

        String testResourcesDir =  "test/resources/" + PACKAGE_NAME.replace(".", "/");

        String testBuildDataDir = "build/test/data";
        // Create the test data directory if needed
        File testDataDir = new File(testBuildDataDir,
                TestRegisteredJarVisibility.class.getCanonicalName());
        testDataDir.mkdirs();

        jarFile = new File(testDataDir, JAR_FILE_NAME);

        File[] javaFiles = new File[]{
                new File(testResourcesDir, "RegisteredJarVisibilityLoader.java"),
                new File(testResourcesDir, "RegisteredJarVisibilitySchema.java")};

        List<File> classFiles = compile(javaFiles);

        // Canonical class name to class file
        Map<String, File> filesToJar = Maps.newHashMap();
        for (File classFile : classFiles) {
            filesToJar.put(
                    PACKAGE_NAME + "." + classFile.getName().replace(".class", ""),
                    classFile);
        }

        jar(filesToJar);

        cluster = MiniCluster.buildCluster();
    }

    @After()
    public void tearDown() {
        cluster.shutDown();
    }

    @Test()
    public void testRegisteredJarVisibilitySchemaNotOnClasspath() {
        boolean exceptionThrown = false;
        try {
            Class.forName("org.apache.pig.test.FooSchema");
        } catch (ClassNotFoundException e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
    }

    @Test()
    public void testRegisteredJarVisibility() throws IOException {
        cluster.getFileSystem().copyFromLocalFile(
                new Path("file://" + INPUT_FILE.getAbsolutePath()), new Path(INPUT_FILE.getName()));
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        String query = "register " + jarFile.getAbsolutePath() + ";\n"
                + "a = load '" + INPUT_FILE.getName()
                + "' using org.apache.pig.test.RegisteredJarVisibilityLoader();";
        LOG.info("Running pig script:\n" + query);
        pigServer.registerScript(new ByteArrayInputStream(query.getBytes()));

        pigServer.openIterator("a");
        pigServer.shutdown();
    }

    private List<File> compile(File[] javaFiles) {
        LOG.info("Compiling: " + Arrays.asList(javaFiles));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits1 =
                fileManager.getJavaFileObjects(javaFiles);
        JavaCompiler.CompilationTask task =
                compiler.getTask(null, fileManager, null, null, null, compilationUnits1);
        task.call();

        List<File> classFiles = Lists.newArrayList();
        for (File javaFile : javaFiles) {
            File classFile = new File(javaFile.getAbsolutePath().replace(".java", ".class"));
            classFile.deleteOnExit();
            Assert.assertTrue(classFile.exists());
            classFiles.add(classFile);
            LOG.info("Created " + classFile.getAbsolutePath());
        }

        return classFiles;
    }

    /**
     * Create a jar file containing the generated classes.
     *
     * @param filesToJar map of canonical class name to class file
     * @throws IOException on error
     */
    private void jar(Map<String, File> filesToJar) throws IOException {
        LOG.info("Creating jar file containing: " + filesToJar);

        JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile.getAbsolutePath()));
        try {
            for (Map.Entry<String, File> entry : filesToJar.entrySet()) {
                String zipEntryName = entry.getKey().replace(".", "/") + ".class";
                LOG.info("Adding " + zipEntryName + " to " + jarFile.getAbsolutePath());
                jos.putNextEntry(new ZipEntry(zipEntryName));
                InputStream classInputStream = new FileInputStream(entry.getValue().getAbsolutePath());
                try {
                    ByteStreams.copy(classInputStream, jos);
                } finally {
                    classInputStream.close();
                }
            }
        } finally {
            jos.close();
        }
        Assert.assertTrue(jarFile.exists());
        LOG.info("Created " + jarFile.getAbsolutePath());
    }
}
