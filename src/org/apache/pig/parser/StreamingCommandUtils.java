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

package org.apache.pig.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.streaming.StreamingCommand;

// Check and set files to be automatically shipped for the given StreamingCommand
// Auto-shipping rules:
// 1. If the command begins with either perl or python assume that the 
//    binary is the first non-quoted string it encounters that does not 
//    start with dash - subject to restrictions in (2).
// 2. Otherwise, attempt to ship the first string from the command line as 
//    long as it does not come from /bin, /user/bin, /user/local/bin. 
//    It will determine that by scanning the path if an absolute path is 
//    provided or by executing "which". The paths can be made configurable 
//    via "set stream.skippath <paths>" option.
public class StreamingCommandUtils {
    private static final String PERL = "perl";
    private static final String PYTHON = "python";
    private static final char SINGLE_QUOTE = '\u005c'';
    private static final char DOUBLE_QUOTE = '"';
    
    private final PigContext pigContext;
    
    public StreamingCommandUtils(PigContext pigContext) {
        this.pigContext = pigContext;
    }
    
    static String[] splitArgs(String command) throws ParseException {
        List<String> argv = new ArrayList<String>();

        int beginIndex = 0;

        while (beginIndex < command.length()) {
            // Skip spaces
            while (Character.isWhitespace(command.charAt(beginIndex))) {
                ++beginIndex;
            }

            char delim = ' ';
            char charAtIndex = command.charAt(beginIndex);
            if (charAtIndex == SINGLE_QUOTE || charAtIndex == DOUBLE_QUOTE) {
                delim = charAtIndex;
            }

            int endIndex = command.indexOf(delim, beginIndex+1);
            if (endIndex == -1) {
                if (Character.isWhitespace(delim)) {
                    // Reached end of command-line
                    argv.add(command.substring(beginIndex));
                    break;
                } else {
                    // Didn't find the ending quote/double-quote
                    throw new ParseException("Illegal command: " + command);
                }
            }

            if (Character.isWhitespace(delim)) {
                // Do not consume the space
                argv.add(command.substring(beginIndex, endIndex));
            } else {
                argv.add(command.substring(beginIndex, endIndex+1));
            }

            beginIndex = endIndex + 1;
        }

        return argv.toArray(new String[argv.size()]);
    }

    void checkAutoShipSpecs(StreamingCommand command, String[] argv) 
    throws ParseException {
        // Candidate for auto-ship
        String arg0 = argv[0];
        
        // Check if command is perl or python ... if so use the first non-option
        // and non-quoted string as the candidate
       if (arg0.equalsIgnoreCase(PERL) || arg0.equalsIgnoreCase(PYTHON)) {
           for (int i=1; i < argv.length; ++i) {
               if (!argv[i].startsWith("-") && !isQuotedString(argv[i])) {
                   checkAndShip(command, argv[i]);
                   break;
               }
           }
       } else {
           // Ship the first argument if it can be ...
           checkAndShip(command, arg0);
       }
    }
    
    private void checkAndShip(StreamingCommand command, String arg) 
    throws ParseException {
        // Don't auto-ship if it is an absolute path...
        if (arg.startsWith("/")) {
            return;
        }
        
        // $ which arg
        String argPath = which(arg);
        if (argPath != null && !inSkipPaths(argPath)) {
            try {
                command.addPathToShip(argPath);
            } catch(IOException e) {
               ParseException pe = new ParseException(e.getMessage());
               pe.initCause(e);
               throw pe;
           }
        }
         
    }

    private static boolean isQuotedString(String s) {
         return (s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\'');
     }
     
     // Check if file is in the list paths to be skipped 
     private boolean inSkipPaths(String file) {
         for (String skipPath : pigContext.getPathsToSkip()) {
             if (file.startsWith(skipPath)) {
                 return true;
             }
         }
        return false;
     }

     private static String which(String file) {
         try {
             ProcessBuilder processBuilder = 
                 new ProcessBuilder(new String[] {"which", file});
             Process process = processBuilder.start();
     
             BufferedReader stdout = 
                 new BufferedReader(new InputStreamReader(process.getInputStream()));
             String fullPath = stdout.readLine();

             return (process.waitFor() == 0) ? fullPath : null;
         } catch (Exception e) {}
         return null;
      }

}
