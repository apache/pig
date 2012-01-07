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

package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.hsqldb.Server;
import org.junit.After;
import org.junit.Before;

import junit.framework.TestCase;

public class TestDBStorage extends TestCase {

	private PigServer pigServer;
	private MiniCluster cluster;
	private Server dbServer;
	private String driver = "org.hsqldb.jdbcDriver";
	// private String url = "jdbc:hsqldb:mem:.";
	private String dblocation = "/tmp/batchtest";
	private String url = "jdbc:hsqldb:file:" + dblocation
			+ ";hsqldb.default_table_type=cached;hsqldb.cache_rows=100";
	  private String dbUrl = "jdbc:hsqldb:hsql://localhost/" + "batchtest";
	private String user = "sa";
	private String password = "";

	private static final String INPUT_FILE = "datafile.txt";

	public TestDBStorage() throws ExecException, IOException {
		// Initialise Pig server
		cluster = MiniCluster.buildCluster();
		pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
		pigServer.getPigContext().getProperties()
				.setProperty("mapred.map.max.attempts", "1");
		pigServer.getPigContext().getProperties()
				.setProperty("mapred.reduce.max.attempts", "1");
		System.out.println("Pig server initialized successfully");
		// Initialise DBServer
		dbServer = new Server();
		dbServer.setDatabaseName(0, "batchtest");
		// dbServer.setDatabasePath(0, "mem:test;sql.enforce_strict_size=true");
		dbServer.setDatabasePath(0,
				"file:/tmp/batchtest;sql.enforce_strict_size=true");
		dbServer.setLogWriter(null);
		dbServer.setErrWriter(null);
		dbServer.start();                                                                     
		System.out.println("Database URL: " + dbUrl);     
		try {
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(this + ".setUp() error: " + e.getMessage());
		}
		System.out.println("Database server started on port: " + dbServer.getPort()); 
	}

	private void createFile() throws IOException {
		PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
		w = new PrintWriter(new FileWriter(INPUT_FILE));
		w.println("100\tapple\t1.0");
		w.println("100\torange\t2.0");
		w.println("100\tbanana\t1.1");
		w.close();
		Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
	}

	private void createTable() throws IOException {
		Connection con = null;
		String sql = "create table ttt (id integer, name varchar(32), ratio double)";
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException sqe) {
			throw new IOException("Unable to obtain a connection to the database",
					sqe);
		}
		try {
			Statement st = con.createStatement();
			st.executeUpdate(sql);
			st.close();
			 con.commit();
			con.close();
		} catch (SQLException sqe) {
			throw new IOException("Cannot create table", sqe);
		}
	}

	@Before
	public void setUp() throws IOException {
		createFile();
		createTable();
	}

	@After
	public void tearDown() throws IOException {
		new File(INPUT_FILE).delete();
		Util.deleteFile(cluster, INPUT_FILE);
		pigServer.shutdown();
		dbServer.stop();

		File[] dbFiles = new File("/tmp").listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.startsWith("batchtest")) {
					return true;
				} else {
					return false;
				}
			}
		});
		if (dbFiles != null) {
			for (File file : dbFiles) {
				file.delete();
			}
		}
	}

	public void testWriteToDB() throws IOException {
		String insertQuery = "insert into ttt (id, name, ratio) values (?,?,?)";
		pigServer.setBatchOn();
		String dbStore = "org.apache.pig.piggybank.storage.DBStorage('" + driver                                                                                                                       
	            + "', '" + url + "', '" + insertQuery + "');";
		pigServer.registerQuery("A = LOAD '" + INPUT_FILE
				+ "' as (id:int, fruit:chararray, ratio:double);");
		pigServer.registerQuery("STORE A INTO 'dummy' USING " + dbStore);
	  ExecJob job = pigServer.executeBatch().get(0);
		try {
			while(!job.hasCompleted()) Thread.sleep(1000);
		} catch(InterruptedException ie) {// ignore
		}
	  
		assertNotSame("Failed: " + job.getException(), job.getStatus(),
						ExecJob.JOB_STATUS.FAILED);
		
		Connection con = null;
		String selectQuery = "select id, name, ratio from ttt order by name";
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException sqe) {
			throw new IOException(
					"Unable to obtain database connection for data verification", sqe);
		}
		try {
			PreparedStatement ps = con.prepareStatement(selectQuery);
			ResultSet rs = ps.executeQuery();

			int expId = 100;
			String[] expNames = { "apple", "banana", "orange" };
			double[] expRatios = { 1.0, 1.1, 2.0 };
			for (int i = 0; i < 3 && rs.next(); i++) {
				assertEquals("Id mismatch", expId, rs.getInt(1));
				assertEquals("Name mismatch", expNames[i], rs.getString(2));
				assertEquals("Ratio mismatch", expRatios[i], rs.getDouble(3), 0.0001);
			}
		} catch (SQLException sqe) {
			throw new IOException(
					"Unable to read data from database for verification", sqe);
		}
	}
}
