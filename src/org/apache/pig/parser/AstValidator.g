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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.NumValCarrier;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
}

@members {

private static Log log = LogFactory.getLog( AstValidator.class );

@Override
protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow) 
throws RecognitionException {
    throw new MismatchedTokenException( ttype, input );
}

@Override
public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
throws RecognitionException {
    throw e;
}

private void validateSchemaAliasName(Set<String> fieldNames, CommonTree node, String name)
throws DuplicatedSchemaAliasException {
    if( fieldNames.contains( name ) ) {
        throw new DuplicatedSchemaAliasException( input, 
            new SourceLocation( (PigParserNode)node ), name );
    } else {
        fieldNames.add( name );
    }
}

private void validateAliasRef(Set<String> aliases, CommonTree node, String alias)
throws UndefinedAliasException {
    if( !aliases.contains( alias ) ) {
        throw new UndefinedAliasException( input, new SourceLocation( (PigParserNode)node ), alias );
    }
}

private void checkDuplication(int count, CommonTree node) throws ParserValidationException {
    if( count > 1 ) {
        throw new ParserValidationException( input, new SourceLocation( (PigParserNode)node ),
            "Duplicated command option" );
    }
}

private Set<String> aliases = new HashSet<String>();

} // End of @members

@rulecatch {
catch(RecognitionException re) {
    throw re;
}
}

query : ^( QUERY statement* )
;

statement : general_statement
          | split_statement
          | realias_statement
;

split_statement : split_clause
;

realias_statement : realias_clause
;

general_statement : ^( STATEMENT ( alias { aliases.add( $alias.name ); } )? op_clause parallel_clause? )
;

realias_clause : ^(REALIAS alias IDENTIFIER)
   {
       aliases.add( $alias.name );
   }
;

parallel_clause : ^( PARALLEL INTEGER )
;

alias returns[String name, CommonTree node]
 : IDENTIFIER
   { 
       $name = $IDENTIFIER.text;
       $node = $IDENTIFIER;
   }
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

define_clause : ^( DEFINE alias ( cmd | func_clause ) )
;

cmd
@init {
    int ship = 0;
    int cache = 0;
    int in = 0;
    int out = 0;
    int error = 0;
}
 : ^( EXECCOMMAND ( ship_clause { checkDuplication( ++ship, $ship_clause.start ); }
                  | cache_clause { checkDuplication( ++cache, $cache_clause.start ); }
                  | input_clause { checkDuplication( ++in, $input_clause.start ); } 
                  | output_clause { checkDuplication( ++out, $output_clause.start ); } 
                  | error_clause { checkDuplication( ++error, $error_clause.start ); }
                  )*
   )
;

ship_clause : ^( SHIP path_list? )
;

path_list : QUOTEDSTRING+
;

cache_clause : ^( CACHE path_list )
;

input_clause : ^( INPUT stream_cmd+ )
;

stream_cmd : ^( STDIN func_clause? )
           | ^( STDOUT func_clause? )
           | ^( QUOTEDSTRING func_clause? )
;

output_clause : ^( OUTPUT stream_cmd+ )
;

error_clause : ^( STDERROR  ( QUOTEDSTRING INTEGER? )? )
;

load_clause : ^( LOAD filename func_clause? as_clause? )
;

filename : QUOTEDSTRING
;

as_clause: ^( AS field_def_list )
;

field_def[Set<String> fieldNames, NumValCarrier nvc] throws DuplicatedSchemaAliasException
 : ^( FIELD_DEF IDENTIFIER { validateSchemaAliasName( fieldNames, $IDENTIFIER, $IDENTIFIER.text ); } type? )
 | ^( FIELD_DEF_WITHOUT_IDENTIFIER type { validateSchemaAliasName ( fieldNames, $FIELD_DEF_WITHOUT_IDENTIFIER, $nvc.makeNameFromDataType ( $type.typev ) ); } )
;

field_def_list throws DuplicatedSchemaAliasException
scope{
    Set<String> fieldNames;
    NumValCarrier nvc;
}
@init {
    $field_def_list::fieldNames = new HashSet<String>();
    $field_def_list::nvc = new NumValCarrier();
}
 : ( field_def[$field_def_list::fieldNames, $field_def_list::nvc] )+
;

type returns [byte typev]
  : simple_type { $typev = $simple_type.typev; }
  | tuple_type { $typev = DataType.TUPLE; }
  | bag_type { $typev = DataType.BAG; }
  | map_type { $typev = DataType.MAP; }
;

simple_type returns [byte typev]
  : BOOLEAN { $typev = DataType.BOOLEAN; }
  | INT { $typev = DataType.INTEGER; }
  | LONG { $typev = DataType.LONG; }
  | FLOAT { $typev = DataType.FLOAT; }
  | DOUBLE { $typev = DataType.DOUBLE; }
  | DATETIME { $typev = DataType.DATETIME; }
  | CHARARRAY { $typev = DataType.CHARARRAY; }
  | BYTEARRAY { $typev = DataType.BYTEARRAY; }
;

tuple_type : ^( TUPLE_TYPE field_def_list? )
;

bag_type : ^( BAG_TYPE IDENTIFIER? tuple_type? )
;

map_type : ^( MAP_TYPE type? )
;

func_clause : ^( FUNC_REF func_name )
            | ^( FUNC func_name func_args? )
;

func_name : eid ( ( PERIOD | DOLLAR ) eid )*
;

func_args_string : QUOTEDSTRING | MULTILINE_QUOTEDSTRING
;

func_args : func_args_string+
;

cube_clause
 : ^( CUBE cube_item )
;

cube_item
 : rel ( cube_by_clause )
;

cube_by_clause
 : ^( BY cube_or_rollup )
;

cube_or_rollup
 : cube_rollup_list+
;

cube_rollup_list
 : ^( ( CUBE | ROLLUP ) cube_by_expr_list )
;

cube_by_expr_list
 : cube_by_expr+
;

cube_by_expr 
 : col_range | expr | STAR 
;

group_clause
scope {
    int arity;
}
@init {
    $group_clause::arity = 0;
}
 : ^( ( GROUP | COGROUP ) group_item+ group_type? partition_clause? )
;

group_type : QUOTEDSTRING 
;

group_item
 : rel ( join_group_by_clause | ALL | ANY ) ( INNER | OUTER )?
   {
       if( $group_clause::arity == 0 ) {
           // For the first input
           $group_clause::arity = $join_group_by_clause.exprCount;
       } else if( $join_group_by_clause.exprCount != $group_clause::arity ) {
           throw new ParserValidationException( input, new SourceLocation( (PigParserNode)$group_item.start ),
               "The arity of the group by columns do not match." );
       }
   }
;

rel : alias {  validateAliasRef( aliases, $alias.node, $alias.name ); }
    | op_clause parallel_clause?
;

flatten_generated_item : ( flatten_clause | col_range | expr | STAR ) field_def_list?
;

flatten_clause : ^( FLATTEN expr )
;

store_clause : ^( STORE rel filename func_clause? )
;

filter_clause : ^( FILTER rel cond )
;

cond : ^( OR cond cond )
     | ^( AND cond cond )
     | ^( NOT cond )
     | ^( NULL expr NOT? )
     | ^( rel_op expr expr )
     | func_eval
     | ^( BOOL_COND expr )     
;

func_eval: ^( FUNC_EVAL func_name real_arg* )
;

real_arg : expr | STAR | col_range
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
     | ^( CAST_EXPR type_cast expr )
     | ^( EXPR_IN_PAREN expr )
;

type_cast : simple_type | map_type | tuple_type_cast | bag_type_cast
;

tuple_type_cast : ^( TUPLE_TYPE_CAST type_cast* )
;

bag_type_cast : ^( BAG_TYPE_CAST tuple_type_cast? )
;

var_expr : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr: func_eval | col_ref | bin_expr
;

dot_proj : ^( PERIOD col_alias_or_index+ )
;

col_alias_or_index : col_alias | col_index
;

col_alias : GROUP | CUBE | IDENTIFIER
;

col_index : DOLLARVAR
;

col_range :  ^(COL_RANGE col_ref? DOUBLE_PERIOD col_ref?)
;


pound_proj : ^( POUND ( QUOTEDSTRING | NULL ) )
;

bin_expr : ^( BIN_EXPR cond expr expr )
;

limit_clause : ^( LIMIT rel ( INTEGER | LONGINTEGER | expr ) )
;

sample_clause : ^( SAMPLE rel ( DOUBLENUMBER | expr ) )
;

order_clause : ^( ORDER rel order_by_clause func_clause? )
;

order_by_clause : STAR ( ASC | DESC )?
                | order_col+
;

order_col : col_range (ASC | DESC)?
          | col_ref ( ASC | DESC )?
;

distinct_clause : ^( DISTINCT rel partition_clause? )
;

partition_clause : ^( PARTITION func_name )
;

cross_clause : ^( CROSS rel_list partition_clause? )
;

rel_list : rel+
;

join_clause
scope {
    int arity;
}
@init {
    $join_clause::arity = 0;
}
 : ^( JOIN join_sub_clause join_type? partition_clause? )
;

join_type : QUOTEDSTRING
;

join_sub_clause
 : join_item ( LEFT | RIGHT | FULL ) OUTER? join_item
 | join_item+
;

join_item
 : ^( JOIN_ITEM rel join_group_by_clause )
   {
       if( $join_clause::arity == 0 ) {
           // For the first input
           $join_clause::arity = $join_group_by_clause.exprCount;
       } else if( $join_group_by_clause.exprCount != $join_clause::arity ) {
           throw new ParserValidationException( input, new SourceLocation( (PigParserNode)$join_item.start ),
               "The arity of the join columns do not match." );
       }
   }
;

join_group_by_clause returns[int exprCount]
@init {
    $exprCount = 0;
}
 : ^( BY ( join_group_by_expr { $exprCount++; } )+ )
;

join_group_by_expr : col_range  | expr | STAR
;

union_clause : ^( UNION ONSCHEMA? rel_list )
;

foreach_clause : ^( FOREACH rel foreach_plan )
;

foreach_plan : ^( FOREACH_PLAN_SIMPLE generate_clause )
             | ^( FOREACH_PLAN_COMPLEX nested_blk )
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
          | nested_cross
          | nested_foreach
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

nested_limit : ^( LIMIT nested_op_input ( INTEGER | expr ) )
;

nested_cross : ^( CROSS nested_op_input_list )
;

nested_foreach : ^( FOREACH nested_op_input generate_clause )
;

nested_op_input : col_ref | nested_proj
;

nested_op_input_list : nested_op_input+
;

stream_clause : ^( STREAM rel ( EXECCOMMAND | IDENTIFIER ) as_clause? )
;

mr_clause : ^( MAPREDUCE QUOTEDSTRING path_list? store_clause load_clause EXECCOMMAND? )
;

split_clause : ^( SPLIT rel split_branch+ split_otherwise? )
;

split_branch
 : ^( SPLIT_BRANCH alias cond )
   {
       aliases.add( $alias.name );
   }
;

split_otherwise 	: ^( OTHERWISE alias )
   {
       aliases.add( $alias.name );
   }
;

col_ref : alias_col_ref | dollar_col_ref
;

alias_col_ref : GROUP | CUBE | IDENTIFIER
;

dollar_col_ref : DOLLARVAR
;

const_expr : literal
;

literal : scalar | map | bag | tuple
;

scalar : num_scalar | QUOTEDSTRING | NULL | TRUE | FALSE
;

num_scalar : MINUS? ( INTEGER | LONGINTEGER | FLOATNUMBER | DOUBLENUMBER )
;

map : ^( MAP_VAL keyvalue* )
;

keyvalue : ^( KEY_VAL_PAIR map_key const_expr )
;

map_key : QUOTEDSTRING
;

bag : ^( BAG_VAL tuple* )
;

tuple : ^( TUPLE_VAL literal* )
;

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid : rel_str_op
    | IMPORT
    | RETURNS
    | DEFINE
    | LOAD
    | FILTER
    | FOREACH
    | CUBE
    | ROLLUP
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
    | BOOLEAN
    | INT
    | LONG
    | FLOAT
    | DOUBLE
    | DATETIME
    | CHARARRAY
    | BYTEARRAY
    | BAG
    | TUPLE
    | MAP
    | IS
    | NULL
    | TRUE
    | FALSE
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
    | TOBAG
    | TOMAP
    | TOTUPLE
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
