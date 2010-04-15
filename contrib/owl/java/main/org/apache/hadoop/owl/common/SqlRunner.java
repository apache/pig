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

package org.apache.hadoop.owl.common;

/* A simple tool to connect and send SQL commands over commandline to the configured database, through jdbc */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SqlRunner {

    private static final String SQL_CMD_SEPARATOR = ";";
    private static final String SQL_COMMENTSEGMENT_REGEX = "--.*$";
    private static final String SQL_WHITESPACE_CHAR = " ";
    private static final String SQL_LINE_HAS_COMMENT_SEGMENT_REGEX = ".*--.*";
    private static final String SQL_WHITESPACES_REGEX = "\\s+";
    private static final String SQL_LINE_HAS_WHITESPACES_REGEX = ".*\\s+.*";

    Connection conn = null;
    String queryString = null;
    Statement st = null;
    ResultSet rs = null;
    int updateCount = 0;

    boolean success = false;
    boolean queryIsSelect = false;

    String jdbcDriver = null;

    public SqlRunner(String queryString) throws OwlException {
        this(
                OwlConfig.getJdbcDriver(),
                OwlConfig.getJdbcUrl(),
                OwlConfig.getJdbcUser(),
                OwlConfig.getJdbcPassword(),
                queryString
        );
    }
    public SqlRunner(String jdbcDriver, String jdbcUri, String jdbcUser, String jdbcPasswd, String queryString) throws OwlException{
        this.jdbcDriver = jdbcDriver;
        this.queryString = queryString;

        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new OwlException(ErrorType.ERROR_DB_JDBC_DRIVER, "Driver ["+jdbcDriver+"]" , e);
        }

        try {
            conn = DriverManager.getConnection(jdbcUri,jdbcUser,jdbcPasswd);
            st = conn.createStatement();
        } catch (SQLException e) {
            throw new OwlException(ErrorType.ERROR_DB_INIT, "Driver ["+jdbcDriver+"] Uri ["+jdbcUri+"]" , e);
        }

        if (queryString.toLowerCase().startsWith("select")){
            queryIsSelect = true;
        }
    }

    public boolean run() throws OwlException {
        try {
            success = st.execute(queryString);
        } catch (SQLException e) {
            throw new OwlException(ErrorType.ERROR_DB_QUERY_EXEC, "Driver ["+ this.jdbcDriver + "] Query ["+ queryString +"]" , e);
        }
        return success;
    }

    protected void finalize() throws OwlException{
        try {
            st.close();
        } catch (SQLException e) {
            throw new OwlException(ErrorType.ERROR_DB_CLOSE, "Driver ["+ this.jdbcDriver + "] Query ["+ queryString +"]" , e);
        }
    }

    public void dump(PrintStream ps) throws OwlException {

        try {
            if (queryIsSelect){
                rs = st.getResultSet();
                ResultSetMetaData meta = rs.getMetaData();
                for (; rs.next(); ) {
                    for (int i = 0; i < meta.getColumnCount(); ++i) {
                        Object o = rs.getObject(i + 1);
                        ps.print("[" + o.toString() + "]");
                    }

                    ps.println("");
                }
            }else{
                updateCount = st.getUpdateCount();
                ps.println("["+updateCount+"] items updated");
            }
        } catch (SQLException e) {
            throw new OwlException(ErrorType.ERROR_DB_QUERY_EXEC, "Driver ["+ this.jdbcDriver + "] Query ["+ queryString +"]" , e);
        }
    }

    public static void main(String args[]) throws FileNotFoundException {

        LogHandler.configureNullLogAppender();

        String[] sqlCommands;

        if ((args.length == 2) && (args[0].equalsIgnoreCase("-f"))){
            List<String> validLines = new ArrayList<String>();

            StringBuilder currcmd = new StringBuilder();
            try {
                BufferedReader input =  new BufferedReader(new FileReader(args[1]));
                String line = null;
                while (( line = input.readLine()) != null){
                    // System.out.println("Read:["+ line + "]");
                    if (line.matches(SQL_LINE_HAS_COMMENT_SEGMENT_REGEX)){
                        line = line.replaceFirst(SQL_COMMENTSEGMENT_REGEX, "");
                        //                        System.out.println("Found comment segment, truncating to ["+line+"]");
                    }
                    line = line.trim();
                    if (line.matches(SQL_LINE_HAS_WHITESPACES_REGEX)){
                        line = line.replaceAll(SQL_WHITESPACES_REGEX, SQL_WHITESPACE_CHAR);
                        // System.out.println("Whitespaces reduced. Now ["+line+"]");
                    }
                    //                    currcmd.append(SQL_WHITESPACE_CHAR);
                    if (line.indexOf(SQL_CMD_SEPARATOR) != -1){
                        String[] cmds = line.split(SQL_CMD_SEPARATOR);
                        // System.out.println("Found semicolon at ["+line.indexOf(SQL_CMD_SEPARATOR)+"]. ["+cmds.length+"] fragments found.");
                        currcmd.append(cmds[0]);
                        for ( int i = 1; (i < cmds.length) ; i++){
                            //                            System.out.println("Processing fragment ["+cmds[i]+"]");
                            validLines.add(currcmd.toString());
                            currcmd = new StringBuilder();
                            currcmd.append(cmds[i]);
                        }
                        if (cmds.length == 1){
                            // trivial case needs to be taken care of
                            // System.out.println("Only one fragment, off we go!");
                            validLines.add(currcmd.toString());
                            currcmd = new StringBuilder();
                        }
                    } else {
                        currcmd.append(line);
                    }
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }

            if (!currcmd.toString().isEmpty()){
                validLines.add(currcmd.toString());
            }

            sqlCommands = validLines.toArray(new String[0]);
        } else {
            sqlCommands = args;
        }

        for (String sqlCommand : sqlCommands){
            sqlCommand = sqlCommand.trim();
            if (!sqlCommand.isEmpty()){
                SqlRunner sr;
                try {
                    System.out.println("SQL> "+sqlCommand);
                    sr = new SqlRunner(sqlCommand);
                    if (sr.run()){
                        sr.dump(System.out);
                    }
                } catch (OwlException e) {
                    System.err.println(e.getLocalizedMessage());
                    System.exit(1);
                }
            }
        }
    }


}
