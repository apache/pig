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
    } else if( e instanceof UndefinedAliasException ) {
        UndefinedAliasException dae = (UndefinedAliasException)e;
        msg = "Alias '"+ dae.getAlias() + "' is not defined";
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

private void validateAliasRef(Set<String> aliases, String alias)
throws UndefinedAliasException {
    if( !aliases.contains( alias ) ) {
        throw new UndefinedAliasException( input, alias );
    } else {
        aliases.add( alias );
    }
}

private Set<String> aliases = new HashSet<String>();

} // End of @members

query : ^( QUERY statement* )
;

statement : general_statement
          | foreach_statement
          | split_statement
;

split_statement : split_clause
;

general_statement : ^( STATEMENT ( alias { aliases.add( $alias.name ); } )? op_clause parallel_clause? )
;

parallel_clause : ^( PARALLEL INTEGER )
;

// We need to handle foreach specifically because of the ending ';', which is not required 
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : ^( STATEMENT ( alias { aliases.add( $alias.name ); } )? foreach_clause )
;

alias returns[String name] : IDENTIFIER { $name = $IDENTIFIER.text; }
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
          | cross_clause
          | join_clause
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

input_clause : ^( INPUT stream_cmd+ )
;

stream_cmd : ^( STDIN func_clause? )
           | ^( STDOUT func_clause? )
           | ^( QUOTEDSTRING func_clause? )
;

output_clause : ^( OUTPUT stream_cmd+ )
;

error_clause : ^( STDERROR  QUOTEDSTRING INTEGER? )
;

load_clause : ^( LOAD filename func_clause? as_clause? )
;

filename : QUOTEDSTRING
;

as_clause: ^( AS field_def_list )
;

field_def[Set<String> fieldNames] throws Exception
 : ^( FIELD_DEF IDENTIFIER { validateSchemaAliasName( fieldNames, $IDENTIFIER.text ); } )
-> ^( FIELD_DEF IDENTIFIER BYTEARRAY )
 | ^( FIELD_DEF IDENTIFIER { validateSchemaAliasName( fieldNames, $IDENTIFIER.text ); } type )
;

field_def_list
scope{
    Set<String> fieldNames;
}
@init {
    $field_def_list::fieldNames = new HashSet<String>();
}
 : ( field_def[$field_def_list::fieldNames] )+
;

type : simple_type | tuple_type | bag_type | map_type
;

simple_type : INT | LONG | FLOAT | DOUBLE | CHARARRAY | BYTEARRAY
;

tuple_type : ^( TUPLE_TYPE field_def_list )
;

bag_type : ^( BAG_TYPE tuple_type? )
;

map_type : MAP_TYPE
;

func_clause : ^( FUNC func_name func_args? )
            | ^( FUNC_REF func_alias )
;

func_name : eid ( ( PERIOD | DOLLAR ) eid )*
;

func_alias : IDENTIFIER
;

func_args : QUOTEDSTRING+
;

group_clause : ^( GROUP group_item+ group_type? )
             | ^( COGROUP group_item+ group_type? )
;

group_type : HINT_COLLECTED | HINT_MERGE | HINT_REGULAR
;

group_item : rel ( join_group_by_clause | ALL | ANY ) ( INNER | OUTER )?
;

rel : alias {  validateAliasRef( aliases, $alias.name ); }
    | op_clause
;

flatten_generated_item : ( flatten_clause | expr | STAR ) field_def_list?
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
     | ^( NULL expr NOT? )
     | ^( rel_op expr expr )
     | func_eval
;

func_eval: ^( FUNC_EVAL func_name real_arg* )
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
     | ^( NEG expr )
     | ^( CAST_EXPR type expr )
     | ^( EXPR_IN_PAREN expr )
;

var_expr : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr: func_eval | col_ref | bin_expr
;

dot_proj : ^( PERIOD col_alias_or_index+ )
;

col_alias_or_index : col_alias | col_index
;

col_alias : GROUP | IDENTIFIER
;

col_index : ^( DOLLAR INTEGER )
;

pound_proj : ^( POUND ( QUOTEDSTRING | NULL ) )
;

bin_expr : ^( BIN_EXPR cond expr expr )
;

limit_clause : ^( LIMIT rel ( INTEGER | LONGINTEGER ) )
;

sample_clause : ^( SAMPLE rel DOUBLENUMBER )
;

order_clause : ^( ORDER rel order_by_clause func_clause? )
;

order_by_clause : STAR ( ASC | DESC )?
                | order_col+
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

join_clause : ^( JOIN join_sub_clause join_type? partition_clause? )
;

join_type : HINT_REPL | HINT_MERGE | HINT_SKEWED | HINT_DEFAULT
;

join_sub_clause : join_item ( LEFT | RIGHT | FULL ) OUTER? join_item
                | ( join_item )+
;

join_item : ^( JOIN_ITEM rel join_group_by_clause )
;

join_group_by_clause : ^( BY join_group_by_expr+ )
;

join_group_by_expr : expr | STAR
;

union_clause : ^( UNION ONSCHEMA? rel_list )
;

foreach_clause : ^( FOREACH rel foreach_plan )
;

foreach_plan : ^( FOREACH_PLAN nested_blk )
             | ^( FOREACH_PLAN_SIMPLE generate_clause parallel_clause? )
;

nested_blk
scope { Set<String> ids; }
@init{ $nested_blk::ids = new HashSet<String>(); }
 : nested_command* generate_clause
;

generate_clause : ^( GENERATE flatten_generated_item+ )
;

nested_command
 : ^( NESTED_CMD IDENTIFIER nested_op )
   {
       $nested_blk::ids.add( $IDENTIFIER.text );
   }
 | ^( NESTED_CMD_ASSI IDENTIFIER expr )
   {
       $nested_blk::ids.add( $IDENTIFIER.text );
   }
;

nested_op : nested_proj
          | nested_filter
          | nested_sort
          | nested_distinct
          | nested_limit
;

nested_proj : ^( NESTED_PROJ col_ref col_ref+ )
;

nested_filter
 : ^( FILTER nested_op_input cond )
;

nested_sort : ^( ORDER nested_op_input  order_by_clause func_clause? )
;

nested_distinct : ^( DISTINCT nested_op_input )
;

nested_limit : ^( LIMIT nested_op_input INTEGER )
;

nested_op_input : col_ref | nested_proj
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

const_expr : literal
;

literal : scalar | map | bag | tuple
;

scalar : INTEGER | LONGINEGER | FLOATNUMBER | DOUBLENUMBER | QUOTEDSTRING | NULL
;

map : ^( MAP_VAL keyvalue* )
;

keyvalue : ^( KEY_VAL_PAIR map_key const_expr )
;

map_key : QUOTEDSTRING | NULL
;

bag : ^( BAG_VAL tuple* )
;

tuple : ^( TUPLE_VAL literal* )
;

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid : rel_str_op
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
    | STDERROR
    | STDIN
    | STDOUT
    | LIMIT
    | SAMPLE
    | LEFT
    | RIGHT
    | FULL
    | IDENTIFIER
;

// relational operator
rel_op : rel_op_eq
       | rel_op_ne
       | rel_op_gt
       | rel_op_gte
       | rel_op_lt
       | rel_op_lte
       | STR_OP_MATCHES
;

rel_op_eq : STR_OP_EQ | NUM_OP_EQ
;

rel_op_ne : STR_OP_NE | NUM_OP_NE
;

rel_op_gt : STR_OP_GT | NUM_OP_GT
;

rel_op_gte : STR_OP_GTE | NUM_OP_GTE
;

rel_op_lt : STR_OP_LT | NUM_OP_LT
;

rel_op_lte : STR_OP_LTE | NUM_OP_LTE
;

rel_str_op : STR_OP_EQ
           | STR_OP_NE
           | STR_OP_GT
           | STR_OP_LT
           | STR_OP_GTE
           | STR_OP_LTE
           | STR_OP_MATCHES
;
