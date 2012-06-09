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
 * Grammar file for Pig tree parser (visitor for printing Pig script from Ast).
 *
 * NOTE: THIS FILE IS BASED ON QueryParser.g, SO IF YOU CHANGE THAT FILE, YOU WILL 
 *       PROBABLY NEED TO MAKE CORRESPONDING CHANGES TO THIS FILE AS WELL.
 */

tree grammar AstPrinter;

options {
    tokenVocab=QueryParser;
    ASTLabelType=CommonTree;
    output=AST;
    backtrack=true;
}

@header {
package org.apache.pig.parser;
}

@members {

public String getResult() { return sb.toString(); }

private StringBuilder sb = new StringBuilder();

} // End of @members

@rulecatch {
catch(RecognitionException re) {
    throw re;
}
}

query : ^( QUERY statement* )
;

statement : general_statement
          | split_statement { sb.append(";\n"); }
          | realias_statement
;

split_statement : split_clause
;

realias_statement : realias_clause
;

// For foreach statement that with complex inner plan.
general_statement 
    : ^( STATEMENT ( alias { sb.append(" = "); } )? 
        op_clause parallel_clause? ) { sb.append(";\n"); }
;

realias_clause : ^(REALIAS alias IDENTIFIER)
;

parallel_clause 
    : ^( PARALLEL INTEGER ) { sb.append(" ").append($PARALLEL.text).append(" ").append($INTEGER.text); }
;

alias 
    : IDENTIFIER { sb.append($IDENTIFIER.text); }
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
          | foreach_clause
          | cube_clause
;

define_clause 
    : ^( DEFINE IDENTIFIER { sb.append($DEFINE.text).append(" ").append($IDENTIFIER.text).append(" "); } 
        ( cmd | func_clause ) )
;

cmd 
    : ^( EXECCOMMAND { sb.append($EXECCOMMAND.text); }
        ( ship_clause | cache_clause | input_clause | output_clause | error_clause )* )
;

ship_clause 
    : ^( SHIP { sb.append(" ").append($SHIP.text).append(" ("); } path_list? { sb.append(")"); } )
;

path_list 
    : a=QUOTEDSTRING { sb.append(" ").append($a.text); }
        (b=QUOTEDSTRING { sb.append(", ").append($b.text); } )*
;

cache_clause 
    : ^( CACHE { sb.append(" ").append($CACHE.text).append(" ("); } path_list { sb.append(")"); } )
;

input_clause 
    : ^( INPUT { sb.append(" ").append($INPUT.text).append("("); } 
        stream_cmd ( { sb.append(", "); } stream_cmd)* { sb.append(")"); } )
;

stream_cmd 
    : ^( STDIN { sb.append($STDIN.text).append(" USING "); } func_clause? )
    | ^( STDOUT { sb.append($STDOUT.text).append(" USING "); } func_clause? )
    | ^( QUOTEDSTRING { sb.append($QUOTEDSTRING.text).append(" USING "); } func_clause? )
;

output_clause 
    : ^( OUTPUT  { sb.append(" ").append($OUTPUT.text).append(" ("); } 
        stream_cmd ( { sb.append(","); } stream_cmd)* { sb.append(")"); } )
;

error_clause 
    : ^( STDERROR { sb.append(" ").append($STDERROR.text).append(" ("); }
        ( QUOTEDSTRING { sb.append($QUOTEDSTRING.text); } (INTEGER { sb.append(" LIMIT ").append($INTEGER); } )? )? { sb.append(")"); } )
;

load_clause 
    : ^( LOAD { sb.append($LOAD.text).append(" "); } filename 
        ( { sb.append(" USING "); } func_clause)? as_clause? )
;

filename 
    : QUOTEDSTRING { sb.append($QUOTEDSTRING.text); }
;

as_clause
    : ^( AS { sb.append(" ").append($AS.text).append(" "); } field_def_list )
;

field_def
    : ^( FIELD_DEF IDENTIFIER { sb.append($IDENTIFIER.text); }  ( {sb.append(":"); }  type)? )
    | ^( FIELD_DEF type )
;

field_def_list
    : { sb.append("("); } field_def ( { sb.append(", "); } field_def )+ { sb.append(")"); }
    | field_def
;

type : simple_type | tuple_type | bag_type | map_type
;

simple_type 
    : BOOLEAN { sb.append($BOOLEAN.text); }
    | INT { sb.append($INT.text); }
    | LONG { sb.append($LONG.text); }
    | FLOAT { sb.append($FLOAT.text); }
    | DOUBLE { sb.append($DOUBLE.text); }
    | CHARARRAY { sb.append($CHARARRAY.text); }
    | BYTEARRAY { sb.append($BYTEARRAY.text); }
;

tuple_type 
    : ^( TUPLE_TYPE field_def_list? )
;

bag_type 
    : ^( BAG_TYPE { sb.append("bag{"); } ( { sb.append("T:"); } IDENTIFIER? tuple_type )? ) { sb.append("}"); } 
;

map_type : ^( MAP_TYPE { sb.append("map["); } type? ) { sb.append("]"); }
;

func_clause 
    : ^( FUNC_REF func_name )
    | ^( FUNC func_name { sb.append("("); } func_args? { sb.append(")"); } )
;

func_name 
    : eid ( ( PERIOD { sb.append($PERIOD.text); } | DOLLAR { sb.append($DOLLAR.text); } ) eid )*
;

func_args 
    : a=QUOTEDSTRING { sb.append($a.text); }
        (b=QUOTEDSTRING { sb.append(", ").append($b.text); } )*
;

cube_clause
  : ^( CUBE { sb.append($CUBE.text).append(" "); } cube_item )
;

cube_item
  : rel ( cube_by_clause )
;

cube_by_clause
    : ^( BY { sb.append(" ").append($BY.text).append(" ("); } 
    cube_by_expr ( { sb.append(", "); } cube_by_expr )* { sb.append(")"); } )
;

cube_by_expr 
    : col_range | expr | STAR { sb.append($STAR.text); }
;

group_clause
    : ^( ( GROUP { sb.append($GROUP.text).append(" "); } | COGROUP { sb.append($COGROUP.text).append(" "); } ) 
        group_item ( { sb.append(", "); } group_item )* 
      ( { sb.append(" USING "); } group_type )? 
      partition_clause?
    )
;

group_type : QUOTEDSTRING { sb.append($QUOTEDSTRING.text); }
;

group_item
    : rel ( join_group_by_clause 
            | ALL { sb.append(" ").append($ALL.text); } | ANY { sb.append(" ").append($ANY.text); } ) 
            ( INNER { sb.append(" ").append($INNER.text); } | OUTER { sb.append(" ").append($OUTER.text); } )?
;

rel 
    : alias 
    | ( { sb.append(" ("); } op_clause parallel_clause? { sb.append(") "); } )
;

flatten_generated_item 
    : ( flatten_clause | col_range | expr | STAR { sb.append(" ").append($STAR.text); } ) ( { sb.append(" AS "); } field_def_list)?
;

flatten_clause 
    : ^( FLATTEN { sb.append($FLATTEN.text).append("("); } expr { sb.append(") "); } )
;

store_clause 
    : ^( STORE { sb.append($STORE.text).append(" "); } rel { sb.append(" INTO "); } filename ( { sb.append(" USING "); } func_clause)? )
;

filter_clause 
    : ^( FILTER { sb.append($FILTER.text).append(" "); } rel { sb.append(" BY ("); } cond { sb.append(")"); } )
;

cond 
    : ^( OR { sb.append("("); } cond { sb.append(") ").append($OR.text).append(" ("); } cond { sb.append(")"); } )
    | ^( AND { sb.append("("); } cond { sb.append(") ").append($AND.text).append(" ("); } cond { sb.append(")"); } )
    | ^( NOT { sb.append(" ").append($NOT.text).append(" ("); } cond { sb.append(")"); } )
    | ^( NULL expr { sb.append(" IS "); } (NOT { sb.append($NOT.text).append(" "); } )?  { sb.append($NULL.text); } )
    | ^( rel_op expr { sb.append(" ").append($rel_op.result).append(" "); } expr )
    | func_eval
    | ^( BOOL_COND expr )
;

func_eval
    : ^( FUNC_EVAL func_name { sb.append("("); } real_arg ( { sb.append(", "); } real_arg)* { sb.append(")"); } )
    | ^( FUNC_EVAL func_name  { sb.append("()"); } )
;

real_arg 
    : expr | STAR { sb.append($STAR.text); } | col_range
;

expr 
    : ^( PLUS expr { sb.append(" ").append($PLUS.text).append(" "); } expr )
    | ^( MINUS expr { sb.append(" ").append($MINUS.text).append(" "); } expr )
    | ^( STAR expr { sb.append(" ").append($STAR.text).append(" "); } expr )
    | ^( DIV expr { sb.append(" ").append($DIV.text).append(" "); } expr )
    | ^( PERCENT expr { sb.append(" ").append($PERCENT.text).append(" "); } expr )
    | ^( CAST_EXPR { sb.append("("); } type { sb.append(")"); } expr )
    | const_expr
    | var_expr
    | ^( NEG { sb.append($NEG.text); } expr )
    | ^( CAST_EXPR { sb.append("("); } type_cast { sb.append(")"); } expr )
    | ^( EXPR_IN_PAREN { sb.append("("); } expr { sb.append(")"); } )
;

type_cast 
    : simple_type | map_type | tuple_type_cast | bag_type_cast
;

tuple_type_cast 
    : ^( TUPLE_TYPE_CAST { sb.append("tuple("); } type_cast ( {sb.append(", "); } type_cast)* {sb.append(")"); } )
    | ^( TUPLE_TYPE_CAST { sb.append("tuple("); } type_cast? {sb.append(")"); } )
;

bag_type_cast 
    : ^( BAG_TYPE_CAST { sb.append("bag{"); } tuple_type_cast? {sb.append("}"); } )
;

var_expr 
    : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr
    : func_eval | col_ref | bin_expr
;

dot_proj 
    : ^( PERIOD { sb.append(".("); } col_alias_or_index ( { sb.append(", "); } col_alias_or_index)*  { sb.append(")"); } )
;

col_alias_or_index : col_alias | col_index
;

col_alias 
    : GROUP { sb.append($GROUP.text); }
    | CUBE { sb.append($CUBE.text); }
    | IDENTIFIER { sb.append($IDENTIFIER.text); }
;

col_index 
    : DOLLARVAR { sb.append($DOLLARVAR.text); }
;

col_range :  ^(COL_RANGE col_ref? { sb.append(".."); } DOUBLE_PERIOD col_ref?)
;

pound_proj 
    : ^( POUND { sb.append($POUND.text); }
        ( QUOTEDSTRING { sb.append($QUOTEDSTRING.text); } | NULL { sb.append($NULL.text); } ) )
;

bin_expr 
    : ^( BIN_EXPR { sb.append(" ("); } cond { sb.append(" ? "); } expr { sb.append(" : "); } expr { sb.append(") "); } )     
;

limit_clause 
    : ^( LIMIT { sb.append($LIMIT.text).append(" "); } rel 
        ( INTEGER { sb.append(" ").append($INTEGER.text); } | LONGINTEGER { sb.append(" ").append($LONGINTEGER.text); } | expr ) )
;

sample_clause 
    : ^( SAMPLE { sb.append($SAMPLE.text).append(" "); } rel ( DOUBLENUMBER { sb.append(" ").append($DOUBLENUMBER.text); } | expr ) )    
;

order_clause 
    : ^( ORDER { sb.append($ORDER.text).append(" "); } rel
        { sb.append(" BY "); } order_by_clause
        ( { sb.append(" USING "); } func_clause )? )
;

order_by_clause 
    : STAR { sb.append($STAR.text); } ( ASC { sb.append(" ").append($ASC.text); } | DESC { sb.append(" ").append($DESC.text); } )?
    | order_col ( { sb.append(", "); } order_col)*
;

order_col 
    : (col_range | col_ref) ( ASC { sb.append(" ").append($ASC.text); } | DESC { sb.append(" ").append($DESC.text); } )?    
;

distinct_clause 
    : ^( DISTINCT { sb.append($DISTINCT.text).append(" "); } rel partition_clause? )
;

partition_clause 
    : ^( PARTITION { sb.append(" ").append($PARTITION.text).append(" BY "); } func_name )    
;

cross_clause 
    : ^( CROSS { sb.append($CROSS.text).append(" "); } rel_list partition_clause? )    
;

rel_list 
    : rel ( { sb.append(", "); } rel)*
;

join_clause
    : ^( JOIN { sb.append($JOIN.text).append(" "); } join_sub_clause ( { sb.append(" USING "); } join_type )? 
    ( partition_clause )? )
;

join_type : QUOTEDSTRING { sb.append($QUOTEDSTRING.text); }
;

join_sub_clause
    : join_item ( LEFT { sb.append(" ").append($LEFT.text); }
             | RIGHT { sb.append(" ").append($RIGHT.text); }
             | FULL { sb.append(" ").append($FULL.text); }
             ) (OUTER { sb.append(" ").append($OUTER.text); } )? { sb.append(", "); } join_item
    | join_item ( { sb.append(", "); } join_item )*
;

join_item
    : ^( JOIN_ITEM rel join_group_by_clause )
;

join_group_by_clause
    : ^( BY { sb.append(" ").append($BY.text).append(" ("); } 
    join_group_by_expr ( { sb.append(", "); } join_group_by_expr )* { sb.append(")"); } )
;

join_group_by_expr 
    : col_range | expr | STAR { sb.append($STAR.text); }
;

union_clause 
    : ^( UNION { sb.append($UNION.text).append(" "); } (ONSCHEMA { sb.append($ONSCHEMA.text).append(" "); } )? rel_list )    
;

foreach_clause 
    : ^( FOREACH { sb.append($FOREACH.text).append(" "); } rel foreach_plan )    
;

foreach_plan 
    : ^( FOREACH_PLAN_SIMPLE generate_clause )
    | ^( FOREACH_PLAN_COMPLEX nested_blk )
;

nested_blk
    : { sb.append(" { "); } (nested_command { sb.append("; "); } )* generate_clause { sb.append("; } "); }
;

generate_clause 
    : ^( GENERATE { sb.append(" ").append($GENERATE.text).append(" "); }
        flatten_generated_item ( { sb.append(", "); } flatten_generated_item)* )    
;

nested_command
    : ^( NESTED_CMD IDENTIFIER { sb.append($IDENTIFIER.text).append(" = "); } nested_op )
    | ^( NESTED_CMD_ASSI IDENTIFIER { sb.append($IDENTIFIER.text).append(" = "); } expr )
;

nested_op : nested_proj
          | nested_filter
          | nested_sort
          | nested_distinct
          | nested_limit
          | nested_cross
          | nested_foreach
;

nested_proj 
    : ^( NESTED_PROJ col_ref { sb.append(".("); } col_ref ( { sb.append(", "); } col_ref)* { sb.append(")"); } )    
;

nested_filter
    : ^( FILTER { sb.append($FILTER.text).append(" "); } nested_op_input { sb.append(" BY "); } cond )    
;

nested_sort 
    : ^( ORDER { sb.append($ORDER.text).append(" "); } nested_op_input
        { sb.append(" BY "); } order_by_clause ( { sb.append(" USING "); } func_clause)? )    
;

nested_distinct 
    : ^( DISTINCT { sb.append($DISTINCT.text).append(" "); }  nested_op_input )    
;

nested_limit 
    : ^( LIMIT { sb.append($LIMIT.text).append(" "); }  nested_op_input ( INTEGER { sb.append(" ").append($INTEGER.text); } | expr ) )
;

nested_cross
    : ^( CROSS { sb.append($CROSS.text).append(" "); }  nested_op_input_list )
;

nested_foreach
	: ^( FOREACH { sb.append($FOREACH.text).append(" "); }  nested_op_input generate_clause )
;

nested_op_input : col_ref | nested_proj
;

nested_op_input_list 
    : nested_op_input ( { sb.append(", "); } nested_op_input)*
;

stream_clause 
    : ^( STREAM { sb.append($STREAM.text).append(" "); } rel { sb.append(" THROUGH "); }
        ( EXECCOMMAND { sb.append($EXECCOMMAND.text); }
        | IDENTIFIER { sb.append($IDENTIFIER.text); } ) as_clause? )
;

mr_clause 
    : ^( MAPREDUCE QUOTEDSTRING { sb.append($MAPREDUCE.text).append(" ").append($QUOTEDSTRING.text).append(" "); }
        ({ sb.append(" ("); } path_list { sb.append(") "); } )? store_clause { sb.append(" "); } load_clause
        (EXECCOMMAND { sb.append(" ").append($EXECCOMMAND.text); } )? )
;

split_clause 
    : ^( SPLIT  { sb.append($SPLIT.text).append(" "); }
        rel { sb.append(" INTO "); } split_branch ( { sb.append(", "); } split_branch )* split_otherwise? )
;

split_branch
    : ^( SPLIT_BRANCH alias { sb.append(" IF "); } cond )    
;

split_otherwise 
    : ^( OTHERWISE { sb.append($OTHERWISE.text).append(" "); } alias ) 
;

col_ref : alias_col_ref | dollar_col_ref
;

alias_col_ref 
    : GROUP { sb.append($GROUP.text); }
    | CUBE { sb.append($CUBE.text); }
    | IDENTIFIER { sb.append($IDENTIFIER.text); }
;

dollar_col_ref 
    : DOLLARVAR { sb.append($DOLLARVAR.text); }
;

const_expr : literal
;

literal : scalar | map | bag | tuple
;

scalar : num_scalar
       | QUOTEDSTRING { sb.append($QUOTEDSTRING.text); }
       | NULL { sb.append($NULL.text); } 
       | TRUE { sb.append($TRUE.text); }
       | FALSE { sb.append($FALSE.text); }   
;

num_scalar : ( MINUS { sb.append( "-" ); } )?
             ( INTEGER { sb.append($INTEGER.text); }
             | LONGINTEGER { sb.append($LONGINTEGER.text); }
             | FLOATNUMBER { sb.append($FLOATNUMBER.text); }
             | DOUBLENUMBER { sb.append($DOUBLENUMBER.text); }
             )
;

map 
    : ^( MAP_VAL { sb.append("["); } keyvalue ( { sb.append(", "); } keyvalue)* { sb.append("]"); } )
    | ^( MAP_VAL { sb.append("[]"); } )
;

keyvalue 
    : ^( KEY_VAL_PAIR map_key { sb.append("#"); } const_expr )    
;

map_key : QUOTEDSTRING { sb.append($QUOTEDSTRING.text); }
;

bag 
    : ^( BAG_VAL { sb.append("{"); } tuple ( { sb.append(", "); } tuple)* { sb.append("}"); } )
    | ^( BAG_VAL { sb.append("{}"); } )
;

tuple 
    : ^( TUPLE_VAL { sb.append("("); } literal ( { sb.append(", "); }  literal)* { sb.append(")"); } )
    | ^( TUPLE_VAL { sb.append("()"); } )
;

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid : rel_str_op
    | IMPORT    { sb.append($IMPORT.text); }
    | RETURNS   { sb.append($RETURNS.text); }
    | DEFINE    { sb.append($DEFINE.text); }
    | LOAD      { sb.append($LOAD.text); }
    | FILTER    { sb.append($FILTER.text); }
    | FOREACH   { sb.append($FOREACH.text); }
    | CUBE      { sb.append($CUBE.text); }
    | MATCHES   { sb.append($MATCHES.text); }
    | ORDER     { sb.append($ORDER.text); }
    | DISTINCT  { sb.append($DISTINCT.text); }
    | COGROUP   { sb.append($COGROUP.text); }
    | JOIN      { sb.append($JOIN.text); }
    | CROSS     { sb.append($CROSS.text); }
    | UNION     { sb.append($UNION.text); }
    | SPLIT     { sb.append($SPLIT.text); }
    | INTO      { sb.append($INTO.text); }
    | IF        { sb.append($IF.text); }
    | ALL       { sb.append($ALL.text); }
    | AS        { sb.append($AS.text); }
    | BY        { sb.append($BY.text); }
    | USING     { sb.append($USING.text); }
    | INNER     { sb.append($INNER.text); }
    | OUTER     { sb.append($OUTER.text); }
    | PARALLEL  { sb.append($PARALLEL.text); }
    | PARTITION { sb.append($PARTITION.text); }
    | GROUP     { sb.append($GROUP.text); }
    | AND       { sb.append($AND.text); }
    | OR        { sb.append($OR.text); }
    | NOT       { sb.append($NOT.text); }
    | GENERATE  { sb.append($GENERATE.text); }
    | FLATTEN   { sb.append($FLATTEN.text); }
    | EVAL      { sb.append($EVAL.text); }
    | ASC       { sb.append($ASC.text); }
    | DESC      { sb.append($DESC.text); }
    | BOOLEAN   { sb.append($BOOLEAN.text); }
    | INT       { sb.append($INT.text); }
    | LONG      { sb.append($LONG.text); }
    | FLOAT     { sb.append($FLOAT.text); }
    | DOUBLE    { sb.append($DOUBLE.text); }
    | CHARARRAY { sb.append($CHARARRAY.text); }
    | BYTEARRAY { sb.append($BYTEARRAY.text); }
    | BAG       { sb.append($BAG.text); }
    | TUPLE     { sb.append($TUPLE.text); }
    | MAP       { sb.append($MAP.text); }
    | IS        { sb.append($IS.text); }
    | NULL      { sb.append($NULL.text); }
    | TRUE      { sb.append($TRUE.text); }
    | FALSE     { sb.append($FALSE.text); }
    | STREAM    { sb.append($STREAM.text); }
    | THROUGH   { sb.append($THROUGH.text); }
    | STORE     { sb.append($STORE.text); }
    | MAPREDUCE { sb.append($MAPREDUCE.text); }
    | SHIP      { sb.append($SHIP.text); }
    | CACHE     { sb.append($CACHE.text); }
    | INPUT     { sb.append($INPUT.text); }
    | OUTPUT    { sb.append($OUTPUT.text); }
    | STDERROR  { sb.append($STDERROR.text); }
    | STDIN     { sb.append($STDIN.text); }
    | STDOUT    { sb.append($STDOUT.text); }
    | LIMIT     { sb.append($LIMIT.text); }
    | SAMPLE    { sb.append($SAMPLE.text); }
    | LEFT      { sb.append($LEFT.text); }
    | RIGHT     { sb.append($RIGHT.text); }
    | FULL      { sb.append($FULL.text); }
    | IDENTIFIER    { sb.append($IDENTIFIER.text); }
    | TOBAG    { sb.append("TOBAG"); }
    | TOMAP    { sb.append("TOMAP"); }
    | TOTUPLE    { sb.append("TOTUPLE"); }
;

// relational operator
rel_op returns[String result]
    : rel_op_eq     { $result = $rel_op_eq.result; }
    | rel_op_ne     { $result = $rel_op_ne.result; }
    | rel_op_gt     { $result = $rel_op_gt.result; }
    | rel_op_gte    { $result = $rel_op_gte.result; }
    | rel_op_lt     { $result = $rel_op_lt.result; }
    | rel_op_lte    { $result = $rel_op_lte.result; }
    | STR_OP_MATCHES  { $result = $STR_OP_MATCHES.text; }
;

rel_op_eq returns[String result]
    : STR_OP_EQ { $result = $STR_OP_EQ.text; }
    | NUM_OP_EQ { $result = $NUM_OP_EQ.text; }
;

rel_op_ne returns[String result]
    : STR_OP_NE { $result = $STR_OP_NE.text; }
    | NUM_OP_NE { $result = $NUM_OP_NE.text; }
;

rel_op_gt returns[String result]
    : STR_OP_GT { $result = $STR_OP_GT.text; }
    | NUM_OP_GT { $result = $NUM_OP_GT.text; }
;

rel_op_gte returns[String result]
    : STR_OP_GTE { $result = $STR_OP_GTE.text; }
    | NUM_OP_GTE { $result = $NUM_OP_GTE.text; }
;

rel_op_lt returns[String result]
    : STR_OP_LT { $result = $STR_OP_LT.text; }
    | NUM_OP_LT { $result = $NUM_OP_LT.text; }
;

rel_op_lte returns[String result]
    : STR_OP_LTE { $result = $STR_OP_LTE.text; }
    | NUM_OP_LTE { $result = $NUM_OP_LTE.text; }
;

rel_str_op
    : STR_OP_EQ { sb.append($STR_OP_EQ.text); }
    | STR_OP_NE { sb.append($STR_OP_NE.text); }
    | STR_OP_GT { sb.append($STR_OP_GT.text); }
    | STR_OP_LT { sb.append($STR_OP_LT.text); }
    | STR_OP_GTE { sb.append($STR_OP_GTE.text); }
    | STR_OP_LTE { sb.append($STR_OP_LTE.text); }
    | STR_OP_MATCHES { sb.append($STR_OP_MATCHES.text); }
;

