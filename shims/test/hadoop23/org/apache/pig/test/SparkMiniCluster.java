package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkExecType;

public class SparkMiniCluster extends MiniGenericCluster {
    private static final File CONF_DIR = new File("build/classes");
    private static final File CONF_FILE = new File(CONF_DIR, "hadoop-site.xml");
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
            if (CONF_FILE.exists()) {
                CONF_FILE.delete();
            }
            m_conf = new Configuration();
            m_conf.set("io.sort.mb", "1");
            m_conf.writeXml(new FileOutputStream(CONF_FILE));
            int dataNodes = 4;
            m_dfs = new MiniDFSCluster(m_conf, dataNodes, true, null);
            m_fileSys = m_dfs.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }

    @Override
    protected void shutdownMiniMrClusters() {
        if (CONF_FILE.exists()) {
            CONF_FILE.delete();
        }
    }

}
