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

tree grammar AliasMasker;

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
}

@members {

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames) {
	if (e instanceof ParserValidationException) {
		return e.toString();
	} 
	return super.getErrorMessage(e, tokenNames);
}

public void setParams(Set ps, String macro, long idx) {
    params = ps; 
    macroName = macro;
    index = idx;
}

private String getMask(String alias) {
    return params.contains( alias ) 
        ? alias 
        : "macro_" + macroName + "_" + alias + "_" + index;
}

private Set<String> params = new HashSet<String>();

private Set<String> aliasSeen = new HashSet<String>();

private String macroName = "";

private long index = 0;

private boolean inAsOrGenClause = false;

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

// For foreach statement that with complex inner plan.
general_statement 
    : ^( STATEMENT ( alias )? 
        op_clause parallel_clause? ) 
;

realias_clause : ^(REALIAS alias IDENTIFIER)
;

parallel_clause 
    : ^( PARALLEL INTEGER ) 
;

alias 
    : IDENTIFIER 
        { 
            aliasSeen.add($IDENTIFIER.text); 
            $IDENTIFIER.getToken().setText(getMask($IDENTIFIER.text)); 
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

define_clause 
    : ^( DEFINE IDENTIFIER  ( cmd | func_clause ) )
;

cmd 
    : ^( EXECCOMMAND 
        ( ship_clause | cache_clause | input_clause | output_clause | error_clause )* )
;

ship_clause 
    : ^( SHIP path_list? )
;

path_list 
    : QUOTEDSTRING+ 
;

cache_clause 
    : ^( CACHE path_list )
;

input_clause 
    : ^( INPUT stream_cmd+ )
;

stream_cmd 
    : ^( STDIN func_clause? )
    | ^( STDOUT func_clause? )
    | ^( QUOTEDSTRING func_clause? )
;

output_clause 
    : ^( OUTPUT stream_cmd+ )
;

error_clause 
    : ^( STDERROR ( QUOTEDSTRING INTEGER? )? )
;

load_clause 
    : ^( LOAD filename func_clause? as_clause? )
;

filename 
    : QUOTEDSTRING 
;

as_clause
@init { 
	inAsOrGenClause = true;
}
@after { 
	inAsOrGenClause = false; 
}
    : ^( AS field_def_list )
;

field_def
    : ^( FIELD_DEF IDENTIFIER type? ) {
	if (inAsOrGenClause) {
		if (aliasSeen.contains($IDENTIFIER.text)) {
			throw new ParserValidationException(input, new SourceLocation((PigParserNode)$field_def.start), 
				"Macro doesn't support user defined schema that contains name that conflicts with alias name: " + $IDENTIFIER.text);
		}
	}
}
    | ^( FIELD_DEF_WITHOUT_IDENTIFIER type )
;

field_def_list
    : field_def+ 
;

type : simple_type | tuple_type | bag_type | map_type
;

simple_type 
    : BOOLEAN | INT | LONG | FLOAT | DOUBLE | CHARARRAY | BYTEARRAY 
;

tuple_type 
    : ^( TUPLE_TYPE field_def_list? )
;

bag_type 
    : ^( BAG_TYPE IDENTIFIER? tuple_type? )
;

map_type : ^( MAP_TYPE type? )
;

func_clause 
    : ^( FUNC_REF func_name )
    | ^( FUNC func_name func_args? )
;

func_name 
    : eid ( ( PERIOD | DOLLAR ) eid )*
;

func_args 
    : QUOTEDSTRING+ 
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
    : ^( ( GROUP | COGROUP ) group_item+ group_type? partition_clause? )
;

group_type : QUOTEDSTRING 
;

group_item
    : rel ( join_group_by_clause | ALL | ANY ) ( INNER | OUTER )?
;

rel 
    : alias | ( op_clause parallel_clause? )
;

flatten_generated_item
@init {
	inAsOrGenClause = true;
} 
@after {
	inAsOrGenClause = false;
}
    : ( flatten_clause | col_range | expr | STAR ) field_def_list?
;

flatten_clause 
    : ^( FLATTEN expr )
;

store_clause 
    : ^( STORE alias filename func_clause? )
;

filter_clause 
    : ^( FILTER rel cond )
;

cond 
    : ^( OR cond cond )
    | ^( AND cond cond )
    | ^( NOT cond )
    | ^( NULL expr NOT? )
    | ^( rel_op expr expr )
    | func_eval
    | ^( BOOL_COND expr )
;

func_eval
    : ^( FUNC_EVAL func_name real_arg* )
;

real_arg 
    : expr | STAR
;

expr 
    : ^( PLUS expr expr )
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

type_cast 
    : simple_type | map_type | tuple_type_cast | bag_type_cast
;

tuple_type_cast 
    : ^( TUPLE_TYPE_CAST type_cast* )
;

bag_type_cast 
    : ^( BAG_TYPE_CAST tuple_type_cast? )
;

var_expr 
    : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr
    : func_eval | col_ref | bin_expr
;

dot_proj 
    : ^( PERIOD col_alias_or_index+ )
;

col_alias_or_index : col_alias | col_index
;

col_alias 
    : GROUP 
    | CUBE 
    | IDENTIFIER
;

col_index 
    : DOLLARVAR
;

col_range :  ^(COL_RANGE col_ref? DOUBLE_PERIOD col_ref?)
;

pound_proj 
    : ^( POUND ( QUOTEDSTRING | NULL ) )
;

bin_expr 
    : ^( BIN_EXPR cond expr expr )     
;

limit_clause 
    : ^( LIMIT rel ( INTEGER | LONGINTEGER | expr ) )
;

sample_clause 
    :	 ^( SAMPLE rel ( DOUBLENUMBER | expr ) )
;

order_clause 
    : ^( ORDER rel order_by_clause func_clause? )
;

order_by_clause 
    : STAR ( ASC | DESC )?
    | order_col+
;

order_col 
    : (col_range | col_ref) ( ASC | DESC )?    
;

distinct_clause 
    : ^( DISTINCT rel partition_clause? )
;

partition_clause 
    : ^( PARTITION func_name )    
;

cross_clause 
    : ^( CROSS rel_list partition_clause? )    
;

rel_list 
    : rel+
;

join_clause
    : ^( JOIN join_sub_clause join_type? partition_clause? )
;

join_type : QUOTEDSTRING
;

join_sub_clause
    : join_item ( LEFT 
             | RIGHT 
             | FULL 
             ) OUTER? join_item
    | join_item+
;

join_item
 : ^( JOIN_ITEM rel join_group_by_clause )
;

join_group_by_clause
    : ^( BY join_group_by_expr+ )
;

join_group_by_expr 
    : col_range | expr | STAR 
;

union_clause 
    : ^( UNION ONSCHEMA? rel_list )    
;

foreach_clause 
    : ^( FOREACH rel foreach_plan )    
;

foreach_plan 
    : ^( FOREACH_PLAN_SIMPLE generate_clause )
    | ^( FOREACH_PLAN_COMPLEX nested_blk )
;

nested_blk
    : nested_command* generate_clause
;

generate_clause 
    : ^( GENERATE flatten_generated_item+ )    
;

nested_command
    : ^( NESTED_CMD IDENTIFIER nested_op )
    | ^( NESTED_CMD_ASSI IDENTIFIER expr )
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
    : ^( NESTED_PROJ col_ref col_ref+ )    
;

nested_filter
    : ^( FILTER nested_op_input cond )    
;

nested_sort 
    : ^( ORDER nested_op_input order_by_clause func_clause? )    
;

nested_distinct 
    : ^( DISTINCT nested_op_input )    
;

nested_limit 
    : ^( LIMIT nested_op_input ( INTEGER | expr ) )
;

nested_cross : ^( CROSS nested_op_input_list )
;

nested_foreach : ^( FOREACH nested_op_input generate_clause )
;

nested_op_input_list : nested_op_input+
;

nested_op_input : col_ref | nested_proj
;

stream_clause 
    : ^( STREAM rel ( EXECCOMMAND | IDENTIFIER ) as_clause? )
;

mr_clause 
    : ^( MAPREDUCE QUOTEDSTRING path_list? store_clause load_clause EXECCOMMAND? )
;

split_clause 
    : ^( SPLIT rel split_branch+ split_otherwise? )
;

split_branch
    : ^( SPLIT_BRANCH alias cond )
;

split_otherwise 
    : ^( OTHERWISE alias ) 
;

col_ref : alias_col_ref | dollar_col_ref
;

alias_col_ref 
    : GROUP 
    | CUBE
    | IDENTIFIER
      {
          String alias = $IDENTIFIER.text;
          String[] names = alias.split( "::" );
          StringBuilder sb = new StringBuilder();
          for( int i = 0; i < names.length; i++ ) {
              String name = names[i];
              sb.append( aliasSeen.contains( name ) ? getMask( name ) : name );
              if( i < names.length - 1 )
                  sb.append( "::" );
          }
          $IDENTIFIER.token.setText( sb.toString() );
      }
;

dollar_col_ref 
    : DOLLARVAR
;

const_expr : literal
;

literal : scalar | map | bag | tuple
;

scalar 
    : INTEGER
    | LONGINTEGER
    | FLOATNUMBER
    | DOUBLENUMBER
    | QUOTEDSTRING
    | NULL
    | TRUE
    | FALSE
;

map 
    : ^( MAP_VAL keyvalue* )
;

keyvalue 
    : ^( KEY_VAL_PAIR map_key const_expr )    
;

map_key : QUOTEDSTRING
;

bag 
    : ^( BAG_VAL tuple* )
;

tuple 
    : ^( TUPLE_VAL literal* )
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
rel_op
    : rel_op_eq
    | rel_op_ne
    | rel_op_gt
    | rel_op_gte
    | rel_op_lt
    | rel_op_lte
    | STR_OP_MATCHES
;

rel_op_eq
    : STR_OP_EQ
    | NUM_OP_EQ
;

rel_op_ne
    : STR_OP_NE
    | NUM_OP_NE
;

rel_op_gt
    : STR_OP_GT
    | NUM_OP_GT
;

rel_op_gte
    : STR_OP_GTE
    | NUM_OP_GTE
;

rel_op_lt
    : STR_OP_LT
    | NUM_OP_LT
;

rel_op_lte
    : STR_OP_LTE
    | NUM_OP_LTE
;

rel_str_op
    : STR_OP_EQ
    | STR_OP_NE
    | STR_OP_GT
    | STR_OP_LT
    | STR_OP_GTE
    | STR_OP_LTE
    | STR_OP_MATCHES
;

