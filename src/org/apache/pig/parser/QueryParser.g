
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
 * Parser file for Pig Parser
 *
 * NOTE: THIS FILE IS THE BASE FOR A FEW TREE PARSER FILES, such as AstValidator.g, 
 *       SO IF YOU CHANGE THIS FILE, YOU WILL PROBABLY NEED TO MAKE CORRESPONDING CHANGES TO 
 *       THOSE FILES AS WELL.
 */

parser grammar QueryParser;

options {
    tokenVocab=QueryLexer;
    output=AST;
    backtrack=true;
}

tokens {
    QUERY;
    STATEMENT;
    FUNC;
    FUNC_REF;
    FUNC_EVAL;
    CAST_EXPR;
    COL_RANGE;
    BIN_EXPR;
    TUPLE_VAL;
    MAP_VAL;
    BAG_VAL;
    KEY_VAL_PAIR;
    FIELD_DEF;
    FIELD_DEF_WITHOUT_IDENTIFIER;
    NESTED_CMD_ASSI;
    NESTED_CMD;
    NESTED_PROJ;
    SPLIT_BRANCH;
    FOREACH_PLAN_SIMPLE;
    FOREACH_PLAN_COMPLEX;
    MAP_TYPE;
    TUPLE_TYPE;
    BAG_TYPE;
    NEG;
    EXPR_IN_PAREN;
    JOIN_ITEM;
    TUPLE_TYPE_CAST;
    BAG_TYPE_CAST;
    PARAMS;
    RETURN_VAL;
    MACRO_DEF;
    MACRO_BODY;
    MACRO_INLINE;
    NULL;
    TRUE;
    FALSE;
    IDENTIFIER;
    ANY;
    TOBAG;
    TOMAP;
    TOTUPLE;
}

@header {
package org.apache.pig.parser;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.parser.PigMacro;
}

@members {
private static Log log = LogFactory.getLog( QueryParser.class );

private Set<String> memory = new HashSet<String>();

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

@Override
public String getErrorMessage(RecognitionException e, String[] tokenNames ) {
    if( !log.isDebugEnabled() ) {
        if( e instanceof NoViableAltException ) {
            return "Syntax error, unexpected symbol at or near " + getTokenErrorDisplay( e.token );
        } else {
            return super.getErrorMessage( e, tokenNames );
        }
    }
    
    List stack =  getRuleInvocationStack( e, this.getClass().getName() );
    String msg = null;
    if( e instanceof NoViableAltException ) {
        NoViableAltException nvae = (NoViableAltException)e;
        msg = " no viable alt; token = " + e.token + " (decision=" + nvae.decisionNumber + " state " + nvae.stateNumber + ")" +
            " decision=<<" + nvae.grammarDecisionDescription + ">>";
    } else {
        msg =  super.getErrorMessage( e, tokenNames );
    }
    return stack + " " + msg;
}

@Override
public String getTokenErrorDisplay(Token t) {
    return "'" + t.getText() + "'";
}

@Override
public String getErrorHeader(RecognitionException ex) {
	return QueryParserUtils.generateErrorHeader( ex, this.getSourceName() );
}

} // End of @members

@rulecatch {
catch(RecognitionException re) {
    throw re;
}
}

query : statement* EOF
     -> ^( QUERY statement* )
;

statement : SEMI_COLON!
          | general_statement
          | foreach_statement
          | split_statement  
          | inline_statement        
          | import_statement
          | realias_statement
;

import_statement : import_clause SEMI_COLON!
;

inline_statement : inline_clause SEMI_COLON!
;

split_statement : split_clause SEMI_COLON!
;

general_statement : ( alias EQUAL )? (op_clause parallel_clause? | LEFT_PAREN op_clause parallel_clause? RIGHT_PAREN) SEMI_COLON 
                 -> ^( STATEMENT alias? op_clause parallel_clause? )
;

realias_statement : realias_clause SEMI_COLON!
;

realias_clause : alias EQUAL identifier 
    -> ^(REALIAS alias identifier)
;

parallel_clause : PARALLEL^ INTEGER
;

// Statement represented by a foreach operator with a nested block. Simple foreach statement
// is covered by general_statement.
// We need to handle foreach specifically because of the ending ';', which is not required 
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : ( ( alias EQUAL )?  FOREACH rel LEFT_CURLY ) => foreach_complex_statement
                  | foreach_simple_statement
;

foreach_complex_statement : ( alias EQUAL )? foreach_clause_complex SEMI_COLON?
                         -> ^( STATEMENT alias? foreach_clause_complex )
;

foreach_simple_statement : ( alias EQUAL )? (foreach_clause_simple parallel_clause? 
                                                | LEFT_PAREN foreach_clause_simple parallel_clause? RIGHT_PAREN) SEMI_COLON
                        -> ^( STATEMENT alias? foreach_clause_simple parallel_clause? )
;

alias : identifier
;

parameter 
    : identifier 
    | INTEGER 
    | DOUBLENUMBER
    | QUOTEDSTRING
    | DOLLARVAR
;

content : LEFT_CURLY ( content | ~(LEFT_CURLY | RIGHT_CURLY) )* RIGHT_CURLY
;

op_clause : define_clause 
          | load_clause
          | group_clause
          | cube_clause
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
;

macro_param_clause : LEFT_PAREN ( alias (COMMA alias)* )? RIGHT_PAREN
    -> ^(PARAMS alias*)
;

macro_return_clause 
    : RETURNS ((alias (COMMA alias)*) | VOID)
        -> ^(RETURN_VAL alias*)
;

macro_body_clause : content
    -> ^(MACRO_BODY { new PigParserNode(new CommonToken(1, $content.text), this.getSourceName(), $content.start) } )
;

macro_clause : macro_param_clause macro_return_clause macro_body_clause
    -> ^(MACRO_DEF macro_param_clause macro_return_clause macro_body_clause)
;

inline_return_clause 
    : alias EQUAL -> ^(RETURN_VAL alias)
	| alias (COMMA alias)+ EQUAL -> ^(RETURN_VAL alias+)
	| -> ^(RETURN_VAL)  
;

inline_param_clause : LEFT_PAREN ( parameter (COMMA parameter)* )? RIGHT_PAREN
    -> ^(PARAMS parameter*)
;

inline_clause : inline_return_clause alias inline_param_clause
    -> ^(MACRO_INLINE alias inline_return_clause inline_param_clause)
;

import_clause : IMPORT^ QUOTEDSTRING
;

define_clause : DEFINE^ alias ( cmd | func_clause | macro_clause)
;

cmd : EXECCOMMAND^ ( ship_clause | cache_clause | input_clause | output_clause | error_clause )*
;

ship_clause : SHIP^ LEFT_PAREN! path_list? RIGHT_PAREN!
;

path_list : QUOTEDSTRING ( COMMA QUOTEDSTRING )* 
         -> QUOTEDSTRING+
;

cache_clause : CACHE^ LEFT_PAREN! path_list RIGHT_PAREN!
;

input_clause : INPUT^ LEFT_PAREN! stream_cmd_list RIGHT_PAREN!
;

stream_cmd_list : stream_cmd ( COMMA stream_cmd )*
               -> stream_cmd+
;

stream_cmd : ( STDIN | STDOUT | QUOTEDSTRING )^ ( USING! ( func_clause ) )?
;

output_clause : OUTPUT^ LEFT_PAREN! stream_cmd_list RIGHT_PAREN!
;

error_clause : STDERROR^ LEFT_PAREN! ( QUOTEDSTRING ( LIMIT! INTEGER )? )? RIGHT_PAREN!
;

load_clause : LOAD^ filename ( USING! func_clause )? as_clause?
;

filename : QUOTEDSTRING
;

as_clause: AS^ ( ( LEFT_PAREN! field_def_list RIGHT_PAREN! ) | field_def )
;

field_def : identifier ( COLON type )?
         -> ^( FIELD_DEF identifier type? )
          | type
         -> ^( FIELD_DEF_WITHOUT_IDENTIFIER type )
;

field_def_list : field_def ( COMMA field_def )*
              -> field_def+
;

type : simple_type | tuple_type | bag_type | map_type
;

simple_type : BOOLEAN | INT | LONG | FLOAT | DOUBLE | CHARARRAY | BYTEARRAY
;

tuple_type : TUPLE? LEFT_PAREN field_def_list? RIGHT_PAREN
          -> ^( TUPLE_TYPE field_def_list? )
;

bag_type : BAG? LEFT_CURLY ( null_keyword COLON tuple_type? ) RIGHT_CURLY
        -> ^( BAG_TYPE tuple_type? )
         | BAG? LEFT_CURLY ( ( identifier COLON )? tuple_type )? RIGHT_CURLY
        -> ^( BAG_TYPE identifier? tuple_type? )
;

map_type : MAP? LEFT_BRACKET type? RIGHT_BRACKET
        -> ^( MAP_TYPE type? )
;

func_clause : func_name
           -> ^( FUNC_REF func_name )
            | func_name LEFT_PAREN func_args? RIGHT_PAREN
           -> ^( FUNC func_name func_args? )
;

func_name : eid ( ( PERIOD | DOLLAR ) eid )*
;

func_args_string : QUOTEDSTRING | MULTILINE_QUOTEDSTRING
;

func_args : func_args_string ( COMMA func_args_string )*
         -> func_args_string+
;

group_clause : ( GROUP | COGROUP )^ group_item_list ( USING! group_type )? partition_clause?
;

group_type : QUOTEDSTRING
;

group_item_list : group_item ( COMMA group_item )*
               -> group_item+
;

group_item : rel ( join_group_by_clause | ALL | ANY ) ( INNER | OUTER )?
;

rel : alias 
    | LEFT_PAREN! ( foreach_clause_complex | ( ( op_clause | foreach_clause_simple ) parallel_clause? ) ) RIGHT_PAREN!
;

flatten_generated_item : flatten_clause ( AS! ( ( LEFT_PAREN! field_def_list RIGHT_PAREN! ) | field_def ) )?
                       | col_range ( AS! ( ( LEFT_PAREN! field_def_list RIGHT_PAREN! ) | field_def ) )?
                       | expr ( AS! field_def )?
                       | STAR ( AS! ( ( LEFT_PAREN! field_def_list RIGHT_PAREN! ) | field_def ) )?
;
	
flatten_clause : FLATTEN^ LEFT_PAREN! expr RIGHT_PAREN!
;

store_clause : STORE^ rel INTO! filename ( USING! func_clause )?
;

filter_clause : FILTER^ rel BY! cond
;

cond : or_cond
;

or_cond : and_cond  ( OR^ and_cond )*
;

and_cond : unary_cond ( AND^ unary_cond )*
;

unary_cond : expr rel_op^ expr
           | LEFT_PAREN! cond RIGHT_PAREN!
           | not_cond           
           | func_eval
           | null_check_cond
           | bool_cond           
;

bool_cond: expr -> ^(BOOL_COND expr);

not_cond : NOT^ unary_cond
;

func_eval : func_name LEFT_PAREN real_arg_list? RIGHT_PAREN
          -> ^( FUNC_EVAL func_name real_arg_list? )
;

real_arg_list : real_arg ( COMMA real_arg )*
             -> real_arg+
;

real_arg : expr | STAR | col_range
;

null_check_cond : expr IS! NOT? null_keyword^
;

expr : add_expr
;

add_expr : multi_expr ( ( PLUS | MINUS )^ multi_expr )*
;

multi_expr : cast_expr ( ( STAR | DIV | PERCENT )^ cast_expr )*
;

cast_expr : LEFT_PAREN type_cast RIGHT_PAREN unary_expr
         -> ^( CAST_EXPR type_cast unary_expr )
          | unary_expr
;

type_cast : simple_type | map_type | tuple_type_cast | bag_type_cast
;

tuple_type_cast : TUPLE LEFT_PAREN ( type_cast ( COMMA type_cast )* )? RIGHT_PAREN
               -> ^( TUPLE_TYPE_CAST type_cast* )
;

bag_type_cast : BAG LEFT_CURLY tuple_type_cast? RIGHT_CURLY
             -> ^( BAG_TYPE_CAST tuple_type_cast? )
;

unary_expr : expr_eval 
           | LEFT_PAREN expr RIGHT_PAREN
          -> ^( EXPR_IN_PAREN expr )
           | neg_expr
;

expr_eval : const_expr | var_expr
;

var_expr : projectable_expr ( dot_proj | pound_proj )*
;

projectable_expr: func_eval | col_ref | bin_expr | type_conversion
;

type_conversion : LEFT_CURLY real_arg_list RIGHT_CURLY 
               -> ^( FUNC_EVAL TOBAG real_arg_list )
               | LEFT_BRACKET real_arg_list RIGHT_BRACKET 
               -> ^( FUNC_EVAL TOMAP real_arg_list )
               | LEFT_PAREN real_arg ( COMMA real_arg )+ RIGHT_PAREN // to disable convertion on 1 element tuples
               -> ^( FUNC_EVAL TOTUPLE real_arg+ )
;

dot_proj : PERIOD ( col_alias_or_index 
                  | ( LEFT_PAREN col_alias_or_index ( COMMA col_alias_or_index )* RIGHT_PAREN ) )
        -> ^( PERIOD col_alias_or_index+ )
;

col_alias_or_index : col_alias | col_index
;

col_alias : GROUP | CUBE | identifier
;

col_index : DOLLARVAR
;

col_range : c1 = col_ref DOUBLE_PERIOD c2 = col_ref?
          -> ^(COL_RANGE $c1 DOUBLE_PERIOD $c2?)
          |  DOUBLE_PERIOD col_ref 
          -> ^(COL_RANGE DOUBLE_PERIOD col_ref)

;

pound_proj : POUND^ ( QUOTEDSTRING | null_keyword )
;

bin_expr : LEFT_PAREN cond QMARK exp1 = expr COLON exp2 = expr RIGHT_PAREN
        -> ^( BIN_EXPR cond $exp1 $exp2 )
;

neg_expr : MINUS cast_expr
        -> ^( NEG cast_expr )
;

limit_clause : LIMIT^ rel ( (INTEGER SEMI_COLON) => INTEGER | (LONGINTEGER SEMI_COLON) => LONGINTEGER | expr )
;

sample_clause : SAMPLE^ rel ( (DOUBLENUMBER SEMI_COLON) => DOUBLENUMBER | expr )
;

order_clause : ORDER^ rel BY! order_by_clause ( USING! func_clause )?
;

order_by_clause : STAR ( ASC | DESC )?
                | order_col_list
;

order_col_list : order_col ( COMMA order_col )*
              -> order_col+
;

order_col : col_range (ASC | DESC)?
          | col_ref ( ASC | DESC )?  
          | LEFT_PAREN! col_ref ( ASC | DESC )? RIGHT_PAREN!
;

distinct_clause : DISTINCT^ rel partition_clause?
;

partition_clause : PARTITION^ BY! func_name
;

cross_clause : CROSS^ rel_list partition_clause?
;

rel_list : rel ( COMMA rel )*
        -> rel+
;

join_clause : JOIN^ join_sub_clause ( USING! join_type )? partition_clause?
;

join_type : QUOTEDSTRING
;

join_sub_clause : join_item ( LEFT | RIGHT | FULL ) OUTER? COMMA! join_item
                | join_item_list
;

join_item_list : join_item ( COMMA! join_item )+
;

join_item : rel join_group_by_clause
         -> ^( JOIN_ITEM  rel join_group_by_clause )
;

join_group_by_clause : BY^ join_group_by_expr_list
;

join_group_by_expr_list : LEFT_PAREN join_group_by_expr ( COMMA join_group_by_expr )* RIGHT_PAREN
                       -> join_group_by_expr+
                        | join_group_by_expr
;

join_group_by_expr : col_range  | expr | STAR
;

union_clause : UNION^ ONSCHEMA? rel_list
;

foreach_clause_simple : FOREACH^ rel foreach_plan_simple
;

foreach_plan_simple : generate_clause
                   -> ^( FOREACH_PLAN_SIMPLE generate_clause )
;

foreach_clause_complex : FOREACH^ rel foreach_plan_complex
;

foreach_plan_complex : nested_blk
                    -> ^( FOREACH_PLAN_COMPLEX nested_blk )
;

cube_clause : CUBE^ cube_item 
;

cube_item : rel ( cube_by_clause )
;

cube_by_clause : BY^ cube_by_expr_list
;

cube_by_expr_list : LEFT_PAREN cube_by_expr ( COMMA cube_by_expr )* RIGHT_PAREN
                       -> cube_by_expr+
                        | cube_by_expr
;

cube_by_expr : col_range  | expr | STAR
;

nested_blk : LEFT_CURLY! nested_command_list ( generate_clause SEMI_COLON! ) RIGHT_CURLY!
;

generate_clause : GENERATE flatten_generated_item ( COMMA flatten_generated_item )*
                  -> ^( GENERATE flatten_generated_item+ )
;

nested_command_list : ( nested_command SEMI_COLON )*
                   -> nested_command*
                    |
;

nested_command : ( identifier EQUAL col_ref PERIOD col_ref_list { input.LA( 1 ) == SEMI_COLON }? ) => ( identifier EQUAL nested_proj )
              -> ^( NESTED_CMD identifier nested_proj )
               | identifier EQUAL expr
              -> ^( NESTED_CMD_ASSI identifier expr )
               | identifier EQUAL nested_op
              -> ^( NESTED_CMD identifier nested_op )
;

nested_op : nested_filter
          | nested_sort
          | nested_distinct
          | nested_limit
          | nested_cross
          | nested_foreach
;

nested_proj : col_ref PERIOD col_ref_list
           -> ^( NESTED_PROJ col_ref col_ref_list )
;

col_ref_list : ( col_ref | ( LEFT_PAREN col_ref ( COMMA col_ref )* RIGHT_PAREN ) )
            -> col_ref+
;

nested_filter : FILTER^ nested_op_input BY! cond
;

nested_sort : ORDER^ nested_op_input BY!  order_by_clause ( USING! func_clause )?
;

nested_distinct : DISTINCT^ nested_op_input
;

nested_limit : LIMIT^ nested_op_input ( (INTEGER SEMI_COLON) => INTEGER | expr )
;

nested_cross : CROSS^ nested_op_input_list
;

nested_foreach: FOREACH^ nested_op_input generate_clause
;

nested_op_input : col_ref | nested_proj
;

nested_op_input_list : nested_op_input ( COMMA nested_op_input )*
        -> nested_op_input+
;

stream_clause : STREAM^ rel THROUGH! ( EXECCOMMAND | alias ) as_clause?
;

mr_clause : MAPREDUCE^ QUOTEDSTRING ( LEFT_PAREN! path_list RIGHT_PAREN! )? store_clause load_clause EXECCOMMAND?
;

split_clause : SPLIT rel INTO split_branch ( ( COMMA split_branch )+ | ( ( COMMA split_branch )* COMMA split_otherwise ) )
            -> ^( SPLIT rel split_branch+ split_otherwise?)
;

split_branch : alias IF cond
            -> ^( SPLIT_BRANCH alias cond )
;

split_otherwise : alias OTHERWISE
            -> ^( OTHERWISE alias )
;

col_ref : alias_col_ref | dollar_col_ref
;

alias_col_ref : GROUP | CUBE | identifier
;

dollar_col_ref : DOLLARVAR
;

const_expr : literal
;

literal : scalar | map | bag | tuple
;


scalar : num_scalar | QUOTEDSTRING | null_keyword | TRUE | FALSE
;

num_scalar : MINUS? ( INTEGER | LONGINTEGER | FLOATNUMBER | DOUBLENUMBER )
;

map : LEFT_BRACKET keyvalue ( COMMA keyvalue )* RIGHT_BRACKET
   -> ^( MAP_VAL keyvalue+ )
    | LEFT_BRACKET RIGHT_BRACKET
   -> ^( MAP_VAL )
;

keyvalue : map_key POUND const_expr
        -> ^( KEY_VAL_PAIR map_key const_expr )
;

map_key : QUOTEDSTRING
;

bag : LEFT_CURLY tuple ( COMMA tuple )* RIGHT_CURLY
   -> ^( BAG_VAL tuple+ )
    | LEFT_CURLY RIGHT_CURLY
   -> ^( BAG_VAL )
;

tuple : LEFT_PAREN literal ( COMMA literal )* RIGHT_PAREN
     -> ^( TUPLE_VAL literal+ )
      | LEFT_PAREN RIGHT_PAREN
     -> ^( TUPLE_VAL )
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
    | ASC
    | DESC
    | BOOL
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
    | identifier
    | null_keyword
    | TRUE
    | FALSE
    | REALIAS
    | BOOL_COND
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

null_keyword : {input.LT(1).getText().equalsIgnoreCase("NULL")}? IDENTIFIER_L
     -> NULL[$IDENTIFIER_L]
;

identifier : {!input.LT(1).getText().equalsIgnoreCase("NULL")}? IDENTIFIER_L
    -> IDENTIFIER[$IDENTIFIER_L]
;
