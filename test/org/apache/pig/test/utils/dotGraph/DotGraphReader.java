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

package org.apache.pig.test.utils.dotGraph;

import org.apache.pig.test.utils.dotGraph.parser.DOTParser;
import org.apache.pig.test.utils.dotGraph.parser.ParseException;

import java.io.*;

/***
 * This class is responsible for loading textual Dot graph
 * into object representation.
 */
public class DotGraphReader {

    /***
     * Load Dot graph from string
     *
     * @param dotContent the Dot content
     * @return graph
     */

    public DotGraph load(String dotContent) {
        ByteArrayInputStream stream
                = new ByteArrayInputStream(dotContent.getBytes()) ;
        DOTParser dotParser = new DOTParser(stream) ;
        DotGraph graph = null ;
        try {
            graph = dotParser.Parse() ;
        }
        catch (ParseException pe) {
            System.out.println(pe.getMessage()) ;
            throw new RuntimeException("Bad Dot file") ;
        }
        return graph ;
    }

    /***
     * Convenient method for loading Dot graph from text file
     * @param file the file containing Dot content
     * @return graph
     */

    public DotGraph loadFromFile(String file) {
        StringBuilder sb = new StringBuilder() ;
        BufferedReader br = null ;
        try {
            br = new BufferedReader(new FileReader(file)) ;
            String str ;
            while((str=br.readLine())!=null) {
                sb.append(str) ;
                sb.append("\n") ;
            }
        }
        catch (FileNotFoundException fnfe) {
            throw new RuntimeException("file:" + file + " not found!") ;
        }
        catch (IOException ioe) {
            throw new RuntimeException("Error while reading from:" + file) ;
        }

        return load(sb.toString()) ;
    }
}
