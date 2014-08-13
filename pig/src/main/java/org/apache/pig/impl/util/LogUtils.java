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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.tools.pigscript.parser.ParseException;

public class LogUtils {
    
    public static void warn(Object o, String msg, PigWarning warningEnum, 
                Log log) {
        
        PigLogger pigLogger = PhysicalOperator.getPigLogger();
        if(pigLogger != null) {
            pigLogger.warn(o, msg, warningEnum);
        } else {
            log.warn(msg); 
        }           
    }
    
    public static Exception getPermissionException(Exception top){
        Throwable current = top;

        while (current != null && (current.getMessage() == null || current.getMessage().indexOf("Permission denied") == -1)){
            current = current.getCause();
        }
        return (Exception)current;
    }
    
    public static PigException getPigException(Throwable top) {
        Throwable current = top;
        Throwable pigException = top;

        if (current instanceof PigException && 
                (((PigException)current).getErrorCode() != 0) && 
                ((PigException) current).getMarkedAsShowToUser()) {
            return (PigException) current;
        }
        while (current != null && current.getCause() != null){
            current = current.getCause();
            if((current instanceof PigException) && 
                    (((PigException)current).getErrorCode() != 0)) {
                pigException = current;
                if (((PigException)pigException).getMarkedAsShowToUser()) {
                    break;
                }
            }
        }
        return (pigException instanceof PigException? (PigException)pigException 
        		: null);        
    }
    
    public static void writeLog(Throwable t, String logFileName, Log log, boolean verbose, String headerMessage) {
        writeLog(t, logFileName, log, verbose, headerMessage, true, true);
    }
    
    public static void writeLog(Throwable t, String logFileName, Log log, boolean verbose, 
            String headerMessage, boolean displayFooter, boolean displayMessage) {
        
        String message = null;
        String marker = null;
        StringBuilder sb = new StringBuilder("=");
        
        for(int i = 0; i < 79; ++i) {
            sb.append("=");
        }
        sb.append("\n");
        marker = sb.toString();
        
        if(t instanceof Exception) {
            Exception pe = LogUtils.getPermissionException((Exception)t);
            if (pe != null) {
                log.error("You don't have permission to perform the operation. Error from the server: " + pe.getMessage());
            }
        }

        PigException pigException = LogUtils.getPigException(t);

        if(pigException != null) {
            message = "ERROR " + pigException.getErrorCode() + ": " + pigException.getMessage();
        } else {
            if((t instanceof ParseException 
                    || t instanceof org.apache.pig.tools.pigscript.parser.TokenMgrError 
                    || t instanceof org.apache.pig.tools.parameters.TokenMgrError)) {
                message = "ERROR 1000: Error during parsing. " + t.getMessage();
            } else if (t instanceof IOException) {
                message = "ERROR 2997: Encountered IOException. " + t.getMessage();
            } else if (t instanceof RuntimeException) {
                message = "ERROR 2999: Unexpected internal error. " + t.getMessage();
            } else {
                message = "ERROR 2998: Unhandled internal error. " + t.getMessage();
            }
        }

        
        FileOutputStream fos = null;
        ByteArrayOutputStream bs = new ByteArrayOutputStream();        
        t.printStackTrace(new PrintStream(bs));
        
        if(displayMessage) log.error(message);
        
        if(verbose) {
            log.error(bs.toString());
        }
        
        if(logFileName == null || logFileName.equals("")) {
            //if exec is invoked programmatically then logFileName will be null
            log.warn("There is no log file to write to.");
            log.error(bs.toString());
            return;
        }
        
        
        File logFile = new File(logFileName);
        try {            
            fos = new FileOutputStream(logFile, true);
            if(headerMessage != null) {
                fos.write((headerMessage + "\n").getBytes("UTF-8"));
                sb = new StringBuilder("-");
                for(int i = 1; i < headerMessage.length(); ++i) {
                    sb.append("-");
                }
                sb.append("\n");
                fos.write(sb.toString().getBytes("UTF-8"));
            }
            if(message != null) {
                if(message.charAt(message.length() - 1) == '\n') {
                    fos.write((message + "\n").getBytes("UTF-8"));
                } else {
                    fos.write((message + "\n\n").getBytes("UTF-8"));
                }
            }
            fos.write(bs.toString().getBytes("UTF-8"));
            fos.write(marker.getBytes("UTF-8"));
            if(displayFooter) {
                if(verbose) {
                    System.err.println("Details also at logfile: " + logFileName);
                } else {
                    System.err.println("Details at logfile: " + logFileName);
                }
            }
        } catch (IOException ioe) {
            log.warn("Could not write to log file: " + logFileName + " :" + ioe.getMessage());
            log.error(bs.toString());
        } finally {
            try {
                if (fos!=null)
                    fos.close();
            } catch (IOException e) {
            }
        }
    }
    
    public static void writeLog(String headerMessage, String message, String logFileName, Log log) {
        if(logFileName == null || logFileName.equals("")) {
            //if exec is invoked programmatically then logFileName will be null
            log.warn("There is no log file to write to.");
            log.error(headerMessage + "\n" + message);
            return;
        }
        
        
        File logFile = new File(logFileName);
        FileOutputStream fos = null;
        try {            
            fos = new FileOutputStream(logFile, true);
            if(headerMessage != null) {
                fos.write((headerMessage + "\n").getBytes("UTF-8"));
                StringBuilder sb = new StringBuilder("-");
                for(int i = 1; i < headerMessage.length(); ++i) {
                    sb.append("-");
                }
                sb.append("\n");
                fos.write(sb.toString().getBytes("UTF-8"));
            }
            if(message != null) {
                if(message.charAt(message.length() - 1) == '\n') {
                    fos.write((message + "\n").getBytes("UTF-8"));
                } else {
                    fos.write((message + "\n\n").getBytes("UTF-8"));
                }
            }
        } catch (IOException ioe) {
            log.warn("Could not write to log file: " + logFileName + " :" + ioe.getMessage());
            log.error(message);
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
            }
        }
    }


}

