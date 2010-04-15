/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.owl.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlResultObject;

/**
 * Command Line client for owl. Removed usage of Apache CLI till CLI-185 is fixed.
 */
public class CommandLineOwl {

    /** The file delimiter character. */
    private static final char DELIMITER_CHAR = ';';

    /** The default URL. */
    private static String     DEFAULT_URL    = "http://localhost:8078/owl/rest";

    /** The member for storing information about the command line arguments. */
    CommandInfo info;

    /**
     * The class for storing information about the command line arguments.
     */
    private static class CommandInfo {
        String command;
        String url = DEFAULT_URL;
        String fileName;
        boolean ignoreErrors = false;
    }

    /**
     * Parses the command line args.
     * 
     * @param args the arguments
     * 
     * @return the command info object
     * 
     * @throws OwlException
     *             the owl exception
     */
    private CommandInfo parseArgs(String[] args) throws OwlException {

        CommandInfo info = new CommandInfo();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-u")) {
                info.url = getNextArg(args, ++i);
            } else if (args[i].equals("-c")) {
                info.command = getNextArg(args, ++i);
            } else if (args[i].equals("-f")) {
                info.fileName = getNextArg(args, ++i);
            } else if (args[i].equals("-i")) {
                info.ignoreErrors = true;
            } else {
                throw new OwlException(ErrorType.INVALID_COMMAND_EXCEPTION,
                        "Unknown argument " + args[i]);
            }
        }

        return info;
    }

    /**
     * Gets the next argument from the current line.
     * 
     * @param args the arguments
     * @param currentArg the current argument
     * 
     * @return the next argument
     * 
     * @throws OwlException
     *             the owl exception
     */
    private String getNextArg(String[] args, int currentArg) throws OwlException {

        if (args.length <= currentArg) {
            throw new OwlException(ErrorType.INVALID_COMMAND_EXCEPTION,
                    "Argument required for " + args[currentArg - 1]);
        }

        String arg = args[currentArg];

        if (arg.charAt(0) == '-') {
            throw new OwlException(ErrorType.INVALID_COMMAND_EXCEPTION,
                    "Invalid argument for " + args[currentArg - 1]);
        }

        return arg;
    }

    /**
     * The main method.
     * 
     * @param args the arguments
     * 
     * @throws Exception
     *             the exception
     */
    public static void main(String[] args) throws Exception {
        int status = (new CommandLineOwl()).run(args);
        System.exit(status);
    }

    /**
     * Run the specified commands.
     * 
     * @param args the arguments
     * @return the exit status
     * @throws Exception
     *             the exception
     */
    public int run(String[] args) throws Exception {

        LogHandler.configureNullLogAppender();

        try {
            // parse the command line arguments, and do validations
            info = parseArgs(args);

            if (info.command != null) {
                executeCommand(info.command, info.url);
            } else if (info.fileName != null) {
                executeFile(info.fileName, info.url);
            } else
                throw new OwlException(ErrorType.INVALID_COMMAND_EXCEPTION,
                "One of -c or -f required");

        } catch (OwlException e) {
            if (e.getErrorType() == ErrorType.INVALID_COMMAND_EXCEPTION) {
                System.err.println(e);
                usage();
            } else {
                System.err.println("ERROR : " + e);
            }
            return 1;
        }

        return 0;
    }


    /**
     * Execute the commands in the specified file.
     * 
     * @param fileName the file name
     * @param url the url
     * 
     * @throws Exception
     *             the exception
     */
    private void executeFile(String fileName, String url) throws Exception {
        FileReader fileReader = new FileReader(fileName);
        BufferedReader reader = new BufferedReader(fileReader);

        boolean eof = false;
        StringBuffer buffer = new StringBuffer(1000);

        while (eof == false) {
            eof = readCommand(reader, buffer);

            if( buffer.length() > 0 ) {
                try { 
                    executeCommand(buffer.toString(), url);
                } catch(OwlException e) {

                    //Client errors always cause the script to stop
                    if( e.getErrorCode() >= ErrorType.ERROR_UNSUPPORTED_ENCODING.getErrorCode() && 
                            e.getErrorCode() < ErrorType.ERROR_UNIMPLEMENTED.getErrorCode() ) {
                        throw e;
                    }

                    //If not client error, stop if ignoreErrors is false
                    if( info.ignoreErrors == false ) {
                        throw e;
                    }

                    //Print error on stderr and continue
                    System.err.println("ERROR : " + e);
                }
            }
        }

        reader.close();
        fileReader.close();
    }


    /**
     * Read a single multi-line command from the file
     * 
     * @param reader the file reader
     * @param buffer  the buffer to append the command to
     * 
     * @return true, if eof reached, false otherwise
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static boolean readCommand(BufferedReader reader, StringBuffer buffer) throws IOException {
        String line = null;
        buffer.setLength(0);

        while ((line = reader.readLine()) != null) {

            // Check if last char is a delimiter
            String trimmedLine = line.trim();
            if( trimmedLine.length() > 0 ) {

                //Check for comments
                if( trimmedLine.substring(0, 2).equals("--") ) {
                    //Ignore comment line
                    continue;
                }

                if (trimmedLine.charAt(trimmedLine.length() - 1) == DELIMITER_CHAR) {
                    //Last char is delimiter, append and stop
                    if( trimmedLine.length() > 1 ) {
                        buffer.append(trimmedLine
                                .substring(0, trimmedLine.length() - 1));
                    }

                    break;
                } else {
                    buffer.append(trimmedLine);
                    buffer.append(" "); //add space so that newline becomes a space
                }
            }
        }

        return (line == null);
    }


    /**
     * Execute specified Owl command.
     * 
     * @param command  the command
     * @param url the url
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void executeCommand(String command, String url) throws OwlException {
        OwlClient client = new OwlClient(url);

        command = command.trim();
        System.err.println(command);
        OwlResultObject result = client.execute(command);

        if (result.getOutput() != null && result.getOutput().size() != 0) {
            printStdout(result.getOutput());
        } else {
            System.err.println("Status : " + result.getStatus());
        }
    }

    /**
     * Prints the output list to stdout.
     * 
     * @param list the list
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void printStdout(List<? extends OwlObject> list) throws OwlException {
        if (list != null) {
            for (OwlObject item : list) {
                printStdout(item);
            }
        }
    }

    /**
     * Prints the owl object to stdout.
     * 
     * @param item the item
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void printStdout(OwlObject item) throws OwlException {
        System.out.println(OwlUtil.getJSONFromObject(item));
    }

    /**
     * Usage help information.
     */
    private void usage() {
        String message = "args : [-u url] [-c command] [-f filename] [-i]\n"
            + "One of -c or -f is mandatory. Default for -u is " + DEFAULT_URL + "\n"
            + "Use -i option to ignore errors for script execution\n";
        System.err.print(message);
    }
}
