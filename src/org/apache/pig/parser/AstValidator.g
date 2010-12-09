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
 * Grammar file for Pig tree parser (visitor for default data type insertion).
 *
 * NOTE: THIS FILE IS BASED ON QueryParser.g, SO IF YOU CHANGE THAT FILE, YOU WILL 
 *       PROBABLY NEED TO MAKE CORRESPONDING CHANGES TO THIS FILE AS WELL.
 */

tree grammar AstValidator;

options {
    tokenVocab=QueryParser;
    ASTLabelType=CommonTree;
    output=AST;
    backtrack=true;
}

@header {
package org.apache.pig.parser;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
}

@members {

private static Log log = LogFactory.getLog( AstValidator.class );

public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg = e.getMessage();
    if ( e instanceof DuplicatedSchemaAliasException ) {
        DuplicatedSchemaAliasException dae = (DuplicatedSchemaAliasException)e;
        msg = "Duplicated schema alias name '"+ dae.getAlias() + "' in the schema definition";
    }
    
    return msg;
}

private void validateSchemaAliasName(Set<String> fieldNames, String name)
throws DuplicatedSchemaAliasException {
    if( fieldNames.contains( name ) ) {
        throw new DuplicatedSchemaAliasException( input, name );
    } else {
        fieldNames.add( name );
    }
}

} // End of @members

query : ^( QUERY statement* )
;

statement : general_statement | foreach_statement
;

general_statement : ^( STATEMENT alias? op_clause INTEGER? )
;

// We need to handle foreach specifically because of the ending ';', which is not required 
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : ^( STATEMENT alias? foreach_clause )
;

alias : IDENTIFIER
;

op_clause : define_clause 
          | load_clause
          | group_clause
          | store_clause
          | filter_clause
          | distinct_clause
          | limit_clause
          | sample_clause
          | order_clause
          | partition_clause
          | cross_clause
          | joint_clause
          | union_clause
          | stream_clause
          | mr_clause
          | split_clause
;

define_clause : ^( DEFINE alias ( cmd | func_clause ) )
;

cmd : ^( EXECCOMMAND ( ship_clause | cache_caluse | input_clause | output_clause | error_clause )* )
;

ship_clause : ^( SHIP path_list? )
;

path_list : QUOTEDSTRING+
;

cache_caluse : ^( CACHE path_list )
;

input_clause : ^( INPUT stream_cmd_list )
;

stream_cmd_list : stream_cmd+
;

stream_cmd : ^( STDIN func_clause? )
           | ^( STDOUT func_clause? )
           | ^( QUOTEDSTRING func_clause? )
;

output_clause : ^( OUTPUT stream_cmd_list )
;

error_clause : ^( ERROR error_cmd? )
;

error_cmd : ^( QUOTEDSTRING INTEGER? )
;

load_clause : ^( LOAD filename func_clause? as_clause? )
;

filename : QUOTEDSTRING
;

as_clause: ^( AS tuple_def )
;

tuple_def : tuple_def_full | tuple_def_simple
;

tuple_def_full
scope{
Set<String> fieldNames;
} : { $tuple_def_full::fieldNames = new HashSet<String>(); }
    ^( TUPLE_DEF field[$tuple_def_full::fieldNames]+ )
;

tuple_def_simple : ^( TUPLE_DEF field[new HashSet<String>()] )
;

field[Set<String> fieldNames] throws Exception
 : ^( FIELD IDENTIFIER { validateSchemaAliasName( fieldNames, $IDENTIFIER.text ); } )
-> ^( FIELD IDENTIFIER BYTEARRAY )
 | ^( FIELD IDENTIFIER { validateSchemaAliasName( fieldNames, $IDENTIFIER.text ); } type ) 
;

type : simple_type | tuple_type | bag_type | map_type
;

simple_type : INT | LONG | FLOAT | DOUBLE | CHARARRAY | BYTEARRAY
;

tuple_type : ^( TUPLE_TYPE tuple_def_full )
;

bag_type : ^( BAG_TYPE tuple_def? )
;

map_type : MAP_TYPE
;

func_clause : ^( FUNC func_name func_args? )
            | ^( FUNC func_alias )
;

func_name : eid+
;

func_alias : IDENTIFIER
;

func_args : QUOTEDSTRING+
;

group_clause : ^( GROUP group_item_list QUOTEDSTRING? )
             | ^( COGROUP group_item_list QUOTEDSTRING? )
;

group_item_list : group_item+
;

group_item : rel ( ( flatten_generated_item_list ) | ALL | ANY ) ( INNER | OUTER )?
;

rel : alias | op_clause
;

flatten_generated_item_list : flatten_generated_item+
;

flatten_generated_item : ( flatten_clause | expr | STAR ) as_clause?
;

flatten_clause : ^( FLATTEN expr )
;

store_clause : ^( STORE alias filename func_clause? )
;

filter_clause : ^( FILTER rel cond )
;

cond : ^( OR cond cond )
     | ^( AND cond cond )
     | ^( NOT cond )
     | ^( NULL expr NOT )
     | ^( FILTEROP expr expr )
     | func_eval
     | ^( NULL expr NOT? )
;

func_eval: ^( FUNC_EVAL func_name real_arg_list? )
;

real_arg_list : real_arg+
;

real_arg : expr | STAR
;

expr : ^( PLUS expr expr )
     | ^( MINUS expr expr )
     | ^( STAR expr expr )
     | ^( DIV expr expr )
     | ^( PERCENT expr expr )
     | ^( CAST_EXPR type expr )
     | const_expr
     | var_expr
     | neg_expr
;

cast_expr : ^( CAST_EXPR type unary_expr )
          | unary_expr
;

unary_expr : expr_eval | expr | neg_expr
;

expr_eval : const_expr | var_expr
;

var_expr : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr: func_eval | col_ref | bin_expr
;

dot_proj : ^( PERIOD col_ref+ )
;

pound_proj : ^( POUND ( QUOTEDSTRING | NULL ) )
;

bin_expr : ^( BIN_EXPR cond expr expr )
;

neg_expr : ^( MINUS cast_expr )
;

limit_clause : ^( LIMIT rel ( INTEGER | LONGINTEGER ) )
;

sample_clause : ^( SAMPLE rel DOUBLENUMBER )
;

order_clause : ^( ORDER rel order_by_clause func_clause? )
;

order_by_clause : STAR ( ASC | DESC )?
                | order_col_list
;

order_col_list : order_col+
;

order_col : col_ref ( ASC | DESC )?
;

distinct_clause : ^( DISTINCT rel partition_clause? )
;

partition_clause : ^( PARTITION func_name )
;

cross_clause : ^( CROSS rel_list partition_clause? )
;

rel_list : rel+
;

joint_clause : ^( JOIN join_sub_clause QUOTEDSTRING? partition_clause? )
;

join_sub_clause : join_item ( LEFT | RIGHT | FULL ) OUTER? join_item
                | join_item_list
;

join_item_list : join_item ( join_item )+
;

join_item : rel flatten_generated_item_list
;

union_clause : ^( UNION ONSCHEMA? rel_list )
;

foreach_clause : ^( FOREACH rel nested_plan )
;

nested_plan : ^( NESTED_PLAN foreach_blk )
            | ^( NESTED_PLAN generate_clause )
;

foreach_blk : nested_command_list generate_clause
;

generate_clause : ^( GENERATE flatten_generated_item+ )
;

nested_command_list : nested_command*
;

nested_command : ^( NESTED_CMD IDENTIFIER expr  )
               | ^( NESTED_CMD IDENTIFIER nested_op )
;

nested_op : nested_proj
          | nested_filter
          | nested_sort
          | nested_distinct
          | nested_limit
;

nested_proj : ^( NESTED_PROJ col_ref col_ref_list )
;

col_ref_list : col_ref+
;

nested_filter : ^( FILTER ( IDENTIFIER | nested_proj | expr_eval ) cond )
;

nested_sort : ^( ORDER ( IDENTIFIER | nested_proj | expr_eval )  order_by_clause func_clause? )
;

nested_distinct : ^( DISTINCT ( IDENTIFIER | nested_proj | expr_eval ) )
;

nested_limit : ^( LIMIT ( IDENTIFIER | nested_proj | expr_eval ) INTEGER )
;

stream_clause : ^( STREAM rel ( EXECCOMMAND | IDENTIFIER ) as_clause? )
;

mr_clause : ^( MAPREDUCE QUOTEDSTRING path_list? store_clause load_clause EXECCOMMAND? )
;

split_clause : ^( SPLIT rel split_branch+ )
;

split_branch : ^( SPLIT_BRANCH IDENTIFIER cond )
;

col_ref : alias_col_ref | dollar_col_ref
;

alias_col_ref : GROUP | IDENTIFIER
;

dollar_col_ref : ^( DOLLAR INTEGER )
;

const_expr : scalar | map | bag | tuple
;

scalar : INTEGER | LONGINEGER | FLOATNUMBER | DOUBLENUMBER | QUOTEDSTRING | NULL
;

map : ^( MAP_VAL keyvalue+ )
;

keyvalue : ^( KEY_VAL_PAIR string_val const_expr )
;

string_val : QUOTEDSTRING | NULL
;

bag : ^( BAG_VAL tuple+ )
;

tuple : ^( TUPLE_VAL const_expr+ )
;

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid : FILTEROP
    | DEFINE
    | LOAD
    | FILTER
    | FOREACH
    | MATCHES
    | ORDER
    | DISTINCT
    | COGROUP
    | JOIN
    | CROSS
    | UNION
    | SPLIT
    | INTO
    | IF
    | ALL
    | AS
    | BY
    | USING
    | INNER
    | OUTER
    | PARALLEL
    | PARTITION
    | GROUP
    | AND
    | OR
    | NOT
    | GENERATE
    | FLATTEN
    | EVAL
    | ASC
    | DESC
    | INT
    | LONG
    | FLOAT
    | DOUBLE
    | CHARARRAY
    | BYTEARRAY
    | BAG
    | TUPLE
    | MAP
    | IS
    | NULL
    | STREAM
    | THROUGH
    | STORE
    | MAPREDUCE
    | SHIP
    | CACHE
    | INPUT
    | OUTPUT
    | ERROR
    | STDIN
    | STDOUT
    | LIMIT
    | SAMPLE
    | LEFT
    | RIGHT
    | FULL
    | IDENTIFIER
;
