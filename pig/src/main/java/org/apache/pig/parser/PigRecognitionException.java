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

import org.antlr.runtime.IntStream;
import org.antlr.runtime.RecognitionException;

/**
 * Subclass of Antlr RecognitionException which should be the parent class of all parser related 
 * exception classes. We need this layer because of the line number problem in tree parser in 
 * Antlr-3.2. You will need a token where the exception occurs in order to instantiate an instance
 * of this class.
 *
 */
public abstract class PigRecognitionException extends RecognitionException {
    private static final long serialVersionUID = 1L;
    
    private SourceLocation location;
    
    public PigRecognitionException(IntStream input, SourceLocation loc) {
        super( );
        this.line = loc.line();
        this.charPositionInLine = loc.offset();
        this.location = loc;
    }
    
    protected String msgHeader() {
        return location.toString();
    }

}
