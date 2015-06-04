package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkExecType;

public class SparkMiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File CORE_CONF_FILE = new File(CONF_DIR, "core-site.xml");
    private static final File HDFS_CONF_FILE = new File(CONF_DIR, "hdfs-site.xml");
    private static final File MAPRED_CONF_FILE = new File(CONF_DIR, "mapred-site.xml");
    private static final File YARN_CONF_FILE = new File(CONF_DIR, "yarn-site.xml");

    private Configuration m_dfs_conf = null;
    protected MiniMRYarnCluster m_mr = null;
    private Configuration m_mr_conf = null;

    private static final Log LOG = LogFactory
            .getLog(SparkMiniCluster.class);
    private ExecType spark = new SparkExecType();
    SparkMiniCluster() {

    }

    @Override
    public ExecType getExecType() {
        return spark;
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
        try {
            CONF_DIR.mkdirs();

            // Build mini DFS cluster
            Configuration hdfsConf = new Configuration();
            m_dfs = new MiniDFSCluster.Builder(hdfsConf)
                    .numDataNodes(2)
                    .format(true)
                    .racks(null)
                    .build();
            m_fileSys = m_dfs.getFileSystem();
            m_dfs_conf = m_dfs.getConfiguration(0);

            //Create user home directory
            m_fileSys.mkdirs(m_fileSys.getWorkingDirectory());
            // Write core-site.xml
            Configuration core_site = new Configuration(false);
            core_site.set(FileSystem.FS_DEFAULT_NAME_KEY, m_dfs_conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
            core_site.writeXml(new FileOutputStream(CORE_CONF_FILE));

            Configuration hdfs_site = new Configuration(false);
            for (Map.Entry<String, String> conf : m_dfs_conf) {
                if (ArrayUtils.contains(m_dfs_conf.getPropertySources(conf.getKey()), "programatically")) {
                    hdfs_site.set(conf.getKey(), m_dfs_conf.getRaw(conf.getKey()));
                }
            }
            hdfs_site.writeXml(new FileOutputStream(HDFS_CONF_FILE));

            // Build mini YARN cluster
            m_mr = new MiniMRYarnCluster("PigMiniCluster", 2);
            m_mr.init(m_dfs_conf);
            m_mr.start();
            m_mr_conf = m_mr.getConfig();
            m_mr_conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    System.getProperty("java.class.path"));

            Configuration mapred_site = new Configuration(false);
            Configuration yarn_site = new Configuration(false);
            for (Map.Entry<String, String> conf : m_mr_conf) {
                if (ArrayUtils.contains(m_mr_conf.getPropertySources(conf.getKey()), "programatically")) {
                    if (conf.getKey().contains("yarn")) {
                        yarn_site.set(conf.getKey(), m_mr_conf.getRaw(conf.getKey()));
                    } else if (!conf.getKey().startsWith("dfs")){
                        mapred_site.set(conf.getKey(), m_mr_conf.getRaw(conf.getKey()));
                    }
                }
            }

            mapred_site.writeXml(new FileOutputStream(MAPRED_CONF_FILE));
            yarn_site.writeXml(new FileOutputStream(YARN_CONF_FILE));

            m_conf = m_mr_conf;
            System.setProperty("junit.hadoop.conf", CONF_DIR.getPath());
            System.setProperty("hadoop.log.dir", "build/test/logs");
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    @Override
    protected void shutdownMiniMrClusters() {
        deleteConfFiles();
        if (m_mr != null) {
            m_mr.stop();
            m_mr = null;
        }
    }

    private void deleteConfFiles() {

        if(CORE_CONF_FILE.exists()) {
            CORE_CONF_FILE.delete();
        }
        if(HDFS_CONF_FILE.exists()) {
            HDFS_CONF_FILE.delete();
        }
        if(MAPRED_CONF_FILE.exists()) {
            MAPRED_CONF_FILE.delete();
        }
        if(YARN_CONF_FILE.exists()) {
            YARN_CONF_FILE.delete();
        }
    }
}
