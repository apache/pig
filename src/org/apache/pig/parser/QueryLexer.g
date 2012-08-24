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
 
/**
 * Lexer file for Pig Parser
 */

lexer grammar QueryLexer;

@header {
package org.apache.pig.parser;
}

@members {

@Override
public void reportError(RecognitionException e) {
    super.reportError( e );
    
    // The method of this signature doesn't permit checked exception. Here we have to 
    // throw a unchecked execption in order to stop at the first error.
    // For more information, visit http://www.antlr.org/wiki/pages/viewpage.action?pageId=5341217.
    StringBuilder sb = new StringBuilder();
    sb.append( getErrorHeader( e ) ).append( " " );
    sb.append( getErrorMessage( e, getTokenNames() ) );
    throw new RuntimeException( sb.toString() );
}

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames ) {
    if( e instanceof NoViableAltException ) {
        return "Unexpected character " + getCharErrorDisplay( e.c );
    } else {
        return super.getErrorMessage( e, tokenNames );
    }
}

@Override
public String getErrorHeader(RecognitionException ex) {
	return QueryParserUtils.generateErrorHeader( ex, this.getSourceName() );
}

} // End of members.

VOID    : 'VOID'
;

IMPORT  : 'IMPORT'
;

RETURNS : 'RETURNS'
;

DEFINE : 'DEFINE'
;

LOAD   : 'LOAD'
;

FILTER : 'FILTER'
;

FOREACH : 'FOREACH'
;

ORDER   :  'ORDER'
;

CUBE    : 'CUBE'
;

ROLLUP	: 'ROLLUP'
;

DISTINCT : 'DISTINCT'
;

COGROUP : 'COGROUP'
;

JOIN : 'JOIN'
;

CROSS : 'CROSS'
;

UNION : 'UNION'
;

SPLIT : 'SPLIT'
;

INTO : 'INTO'
;

IF : 'IF'
;

OTHERWISE : 'OTHERWISE'
;

ALL : 'ALL'
;

AS : 'AS'
;

BY : 'BY'
;

USING : 'USING'
;

INNER : 'INNER'
;

OUTER : 'OUTER'
;

ONSCHEMA : 'ONSCHEMA'
;

PARALLEL : 'PARALLEL'
;

PARTITION : 'PARTITION'
;

GROUP : 'GROUP'
;

AND : 'AND'
;

OR : 'OR'
;

NOT : 'NOT'
;

GENERATE : 'GENERATE'
;

FLATTEN : 'FLATTEN'
;

ASC : 'ASC'
;

DESC : 'DESC'
;

BOOLEAN : 'BOOLEAN'
;

INT : 'INT'
;

LONG : 'LONG'
;

FLOAT : 'FLOAT'
;

DOUBLE : 'DOUBLE'
;

DATETIME : 'DATETIME'
;

CHARARRAY : 'CHARARRAY'
;

BYTEARRAY : 'BYTEARRAY'
;

BAG : 'BAG'
;

TUPLE : 'TUPLE'
;

MAP : 'MAP'
;

IS : 'IS'
;

STREAM : 'STREAM'
;

THROUGH : 'THROUGH'
;

STORE : 'STORE'
;

MAPREDUCE : 'MAPREDUCE'
;

SHIP : 'SHIP'
;

CACHE : 'CACHE'
;

INPUT : 'INPUT'
;

OUTPUT : 'OUTPUT'
;

STDERROR : 'STDERR'
;

STDIN : 'STDIN'
;

STDOUT : 'STDOUT'
;

LIMIT : 'LIMIT'
;

SAMPLE : 'SAMPLE'
;

LEFT : 'LEFT'
;

RIGHT : 'RIGHT'
;

FULL : 'FULL'
;

STR_OP_EQ : 'EQ'
;

STR_OP_GT : 'GT'
;

STR_OP_LT : 'LT'
;

STR_OP_GTE : 'GTE'
;

STR_OP_LTE : 'LTE'
;

STR_OP_NE : 'NEQ'
;

STR_OP_MATCHES : 'MATCHES'
;

TRUE : 'TRUE'
;

FALSE : 'FALSE'
;
    
NUM_OP_EQ : '=='
;

NUM_OP_LT : '<'
;

NUM_OP_LTE : '<='
;

NUM_OP_GT : '>'
;

NUM_OP_GTE : '>=' 
;

NUM_OP_NE : '!='
;
    
fragment DIGIT : '0'..'9'
;

fragment LETTER : 'A'..'Z'
;
    
fragment SPECIALCHAR : '_'
;

fragment ID: LETTER ( DIGIT | LETTER | SPECIALCHAR )*
;

DCOLON : '::'
;

IDENTIFIER_L : ( ID DCOLON ) => ( ID DCOLON IDENTIFIER_L )
           | ID
;

fragment FLOATINGPOINT : INTEGER ( PERIOD INTEGER )? | PERIOD INTEGER 
;
    
INTEGER: ( DIGIT )+
;

LONGINTEGER: INTEGER ( 'L' )?
;

DOLLARVAR : DOLLAR INTEGER
;
    
DOUBLENUMBER : FLOATINGPOINT ( 'E' ( MINUS | PLUS )? INTEGER )?
;
    
FLOATNUMBER : DOUBLENUMBER ( 'F' )?
;

QUOTEDSTRING :  '\'' (   ( ~ ( '\'' | '\\' | '\n' | '\r' ) )
                       | ( '\\' ( ( 'N' | 'T' | 'B' | 'R' | 'F' | '\\' | '\'' ) ) )
                       | ( '\\U' ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )
                                 ( '0'..'9' | 'A'..'F' )  )
                     )*
                '\''
;

MULTILINE_QUOTEDSTRING :  '\'' (   ( ~ ( '\'' | '\\' ) )
                                 | ( '\\' ( ( 'N' | 'T' | 'B' | 'R' | 'F' | '\\' | '\'' | 'n' | 'r' ) ) )
                                 | ( '\\U' ( '0'..'9' | 'A'..'F' )
                                           ( '0'..'9' | 'A'..'F' )
                                           ( '0'..'9' | 'A'..'F' )
                                           ( '0'..'9' | 'A'..'F' )  )
                               )*
                '\''
;

EXECCOMMAND : '`' ( ~( '`' ) )* '`'
;
    
STAR : '*'
;

COLON : ':'
;

DOLLAR : '$'
;
            
WS  :  ( ' ' | '\r' | '\t' | '\u000C' | '\n' ) { $channel = HIDDEN; }
;
    
SL_COMMENT : '--' ( ~( '\r' | '\n' ) )* { $channel = HIDDEN; }
;

ML_COMMENT : '/*' ( options { greedy=false; } : . )* '*/' { $channel = HIDDEN; }
;

SEMI_COLON : ';'
;
    
LEFT_PAREN : '('
;
    
RIGHT_PAREN : ')'
;

LEFT_CURLY : '{'
;

RIGHT_CURLY : '}'
;

LEFT_BRACKET : '['
;
    
RIGHT_BRACKET : ']'
;

POUND : '#'
;

EQUAL : '='
;

COMMA : ','
;

PERIOD : '.'
;

DOUBLE_PERIOD : '..'
;

DIV : '/'
;

PERCENT : '%'
;

PLUS : '+'
;

MINUS : '-'
;

QMARK : '?'
;
