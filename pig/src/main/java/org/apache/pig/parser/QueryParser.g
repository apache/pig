
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
    backtrack=false; // greatly slows down parsing!
}

tokens {
    QUERY;
    STATEMENT;
    FUNC;
    FUNC_REF;
    FUNC_EVAL;
    INVOKE;
    INVOKER_FUNC_EVAL;
    IN_LHS;
    IN_RHS;
    CASE_COND;
    CASE_EXPR;
    CASE_EXPR_LHS;
    CASE_EXPR_RHS;
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
    FAT_ARROW;
}

@header {
package org.apache.pig.parser;

import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.parser.PigMacro;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.base.Joiner;
}

@members {
private static Log log = LogFactory.getLog( QueryParser.class );

private Set<String> memory = new HashSet<String>();

// Make a deep copy of the given node
private static Tree deepCopy(Tree tree) {
    Tree copy = tree.dupNode();
    for (int i = 0; i < tree.getChildCount(); i++) {
        Tree child = deepCopy(tree.getChild(i));
        child.setParent(copy);
        copy.addChild(child);
    }
    return copy;
}

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

private static final Map<Integer, Integer> FUNC_TO_LITERAL = ImmutableMap.of(
    TOBAG, BAG_VAL,
    TOMAP, MAP_VAL,
    TOTUPLE, TUPLE_VAL);

private static final Set<Integer> BOOLEAN_TOKENS = ImmutableSet.of(
    STR_OP_EQ,
    STR_OP_NE,
    STR_OP_GT,
    STR_OP_LT,
    STR_OP_GTE,
    STR_OP_LTE,
    STR_OP_MATCHES,
    AND,
    OR,
    NOT,
    NULL,
    NUM_OP_EQ,
    NUM_OP_NE,
    NUM_OP_GT,
    NUM_OP_GTE,
    NUM_OP_LT,
    NUM_OP_LTE);

private static final Set<Integer> LITERAL_TOKENS = ImmutableSet.of(
    INTEGER,
    LONGINTEGER,
    FLOATNUMBER,
    DOUBLENUMBER,
    QUOTEDSTRING,
    NULL,
    TRUE,
    FALSE,
    MAP_VAL,
    BAG_VAL,
    TUPLE_VAL,
    PERIOD,
    POUND);

} // End of @members

@rulecatch {
catch(RecognitionException re) {
    throw re;
}
}

query : statement* EOF -> ^( QUERY statement* )
;

schema: field_def_list EOF
;

// STATEMENTS

statement : SEMI_COLON!
          | general_statement SEMI_COLON!
          | split_clause SEMI_COLON!
          | inline_clause SEMI_COLON!
          | import_clause SEMI_COLON!
          | realias_clause SEMI_COLON!
          | register_clause SEMI_COLON!
          | assert_clause SEMI_COLON!
          // semicolons after foreach_complex_statement are optional for backwards compatibility, but to keep
          // the grammar unambiguous if there is one then we'll parse it as a single, standalone semicolon
          // (which matches the first statement rule)
          | foreach_statement
;

nested_op_clause : LEFT_PAREN! op_clause parallel_clause? RIGHT_PAREN!
                 | LEFT_PAREN FOREACH rel ( foreach_plan_complex | ( foreach_plan_simple parallel_clause? ) ) RIGHT_PAREN
                    -> ^( FOREACH rel foreach_plan_complex? foreach_plan_simple? ) parallel_clause?
;

general_statement : FAT_ARROW ( ( op_clause parallel_clause? ) | nested_op_clause ) -> ^( STATEMENT IDENTIFIER["____RESERVED____"] op_clause? parallel_clause? nested_op_clause? )
                  | ( identifier_plus EQUAL )? ( ( op_clause parallel_clause? ) | nested_op_clause ) -> ^( STATEMENT identifier_plus? op_clause? parallel_clause? nested_op_clause? )
;

// Statement represented by a foreach operator with a nested block. Simple foreach statement
// is covered by general_statement.
// We need to handle foreach specifically because of the ending ';', which is not required
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : FAT_ARROW FOREACH rel ( foreach_plan_complex | ( foreach_plan_simple parallel_clause? SEMI_COLON ) )
    -> ^( STATEMENT IDENTIFIER["____RESERVED____"] ^( FOREACH rel foreach_plan_complex? foreach_plan_simple? ) parallel_clause? )
                  | ( identifier_plus EQUAL )? FOREACH rel ( foreach_plan_complex | ( foreach_plan_simple parallel_clause? SEMI_COLON ) )
    -> ^( STATEMENT identifier_plus? ^( FOREACH rel foreach_plan_complex? foreach_plan_simple? ) parallel_clause? )
;

foreach_plan_complex : LEFT_CURLY nested_blk RIGHT_CURLY -> ^( FOREACH_PLAN_COMPLEX nested_blk )
;

foreach_plan_simple : GENERATE flatten_generated_item ( COMMA flatten_generated_item )* -> ^( FOREACH_PLAN_SIMPLE ^( GENERATE flatten_generated_item+ ) )
;

// MACRO grammar

macro_content : LEFT_CURLY ( macro_content | ~(LEFT_CURLY | RIGHT_CURLY) )* RIGHT_CURLY
;

macro_param_clause : LEFT_PAREN ( identifier_plus (COMMA identifier_plus)* )? RIGHT_PAREN
    -> ^(PARAMS identifier_plus*)
;

macro_return_clause
    : RETURNS ((identifier_plus (COMMA identifier_plus)*) | VOID)
        -> ^(RETURN_VAL identifier_plus*)
;

macro_body_clause : macro_content -> ^(MACRO_BODY { new PigParserNode(new CommonToken(1, $macro_content.text), this.getSourceName(), $macro_content.start) } )
;

macro_clause : macro_param_clause macro_return_clause macro_body_clause
    -> ^(MACRO_DEF macro_param_clause macro_return_clause macro_body_clause)
;

inline_return_clause
    : identifier_plus EQUAL -> ^(RETURN_VAL identifier_plus)
	| identifier_plus (COMMA identifier_plus)+ EQUAL -> ^(RETURN_VAL identifier_plus+)
	| -> ^(RETURN_VAL)
;

parameter
    : IDENTIFIER
    | INTEGER
    | DOUBLENUMBER
    | BIGDECIMALNUMBER
    | BIGINTEGERNUMBER
    | QUOTEDSTRING
    | DOLLARVAR
;

inline_param_clause : LEFT_PAREN ( parameter (COMMA parameter)* )? RIGHT_PAREN
    -> ^(PARAMS parameter*)
;

inline_clause : inline_return_clause identifier_plus inline_param_clause
    -> ^(MACRO_INLINE identifier_plus inline_return_clause inline_param_clause)
;

// TYPES

simple_type : BOOLEAN | INT | LONG | FLOAT | DOUBLE | DATETIME | BIGINTEGER | BIGDECIMAL | CHARARRAY | BYTEARRAY
;

implicit_tuple_type : LEFT_PAREN field_def_list? RIGHT_PAREN -> ^( TUPLE_TYPE field_def_list? )
;

explicit_tuple_type : TUPLE! implicit_tuple_type
;

explicit_tuple_type_cast : TUPLE LEFT_PAREN ( explicit_type_cast ( COMMA explicit_type_cast )* )? RIGHT_PAREN
    -> ^( TUPLE_TYPE_CAST explicit_type_cast* )
;

tuple_type : implicit_tuple_type | explicit_tuple_type
;

implicit_bag_type : LEFT_CURLY NULL COLON tuple_type? RIGHT_CURLY -> ^( BAG_TYPE tuple_type? )
                  | LEFT_CURLY ( ( identifier_plus COLON )? tuple_type )? RIGHT_CURLY -> ^( BAG_TYPE identifier_plus? tuple_type? )
;

explicit_bag_type : BAG! implicit_bag_type
;

explicit_bag_type_cast : BAG LEFT_CURLY explicit_tuple_type_cast? RIGHT_CURLY -> ^( BAG_TYPE_CAST explicit_tuple_type_cast? )
;

implicit_map_type : LEFT_BRACKET type? RIGHT_BRACKET -> ^( MAP_TYPE type? )
;

explicit_map_type : MAP! implicit_map_type
;

map_type : implicit_map_type | explicit_map_type
;

explicit_type : simple_type | explicit_tuple_type | explicit_bag_type | explicit_map_type
;

implicit_type : implicit_tuple_type | implicit_bag_type | implicit_map_type
;

type : explicit_type | implicit_type
;

explicit_type_cast : simple_type | explicit_map_type | explicit_tuple_type_cast | explicit_bag_type_cast
;

// CLAUSES

import_clause : IMPORT^ QUOTEDSTRING
;

register_clause : REGISTER^ QUOTEDSTRING (USING identifier_plus AS identifier_plus)?
;

define_clause : DEFINE^ IDENTIFIER ( cmd | func_clause | macro_clause)
;

realias_clause : identifier_plus EQUAL identifier_plus -> ^(REALIAS identifier_plus identifier_plus)
;

parallel_clause : PARALLEL^ INTEGER
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
          | rank_clause
          | cross_clause
          | join_clause
          | union_clause
          | stream_clause
          | mr_clause
;

ship_clause : SHIP^ LEFT_PAREN! path_list? RIGHT_PAREN!
;

path_list : QUOTEDSTRING ( COMMA QUOTEDSTRING )* -> QUOTEDSTRING+
;

cache_clause : CACHE^ LEFT_PAREN! path_list RIGHT_PAREN!
;

input_clause : INPUT^ LEFT_PAREN! stream_cmd_list RIGHT_PAREN!
;

output_clause : OUTPUT^ LEFT_PAREN! stream_cmd_list RIGHT_PAREN!
;

error_clause : STDERROR^ LEFT_PAREN! ( QUOTEDSTRING ( LIMIT! INTEGER )? )? RIGHT_PAREN!
;

load_clause : LOAD^ QUOTEDSTRING ( USING! func_clause )? as_clause?
;

func_clause : func_name
           -> ^( FUNC_REF func_name )
            | func_name LEFT_PAREN func_args? RIGHT_PAREN
           -> ^( FUNC func_name func_args? )
;

// needed for disambiguation when parsing expressions...see below
func_name_without_columns : eid_without_columns ( ( PERIOD | DOLLAR ) eid )*
;

func_name : eid ( ( PERIOD | DOLLAR ) eid )*
;

func_args_string : QUOTEDSTRING | MULTILINE_QUOTEDSTRING
;

func_args : func_args_string ( COMMA func_args_string )*
         -> func_args_string+
;

group_clause : ( GROUP | COGROUP )^ group_item_list ( USING! QUOTEDSTRING )? partition_clause?
;

group_item_list : group_item ( COMMA group_item )*
               -> group_item+
;

group_item : rel ( join_group_by_clause | ALL | ANY ) ( INNER | OUTER )?
;

// "AS" CLAUSES

identifier_plus : IDENTIFIER | reserved_identifier_whitelist -> IDENTIFIER[$reserved_identifier_whitelist.text]
;

explicit_field_def : identifier_plus ( COLON type )? -> ^( FIELD_DEF identifier_plus type? )
                   | explicit_type -> ^( FIELD_DEF_WITHOUT_IDENTIFIER explicit_type )
;

field_def : explicit_field_def
          | implicit_type -> ^( FIELD_DEF_WITHOUT_IDENTIFIER implicit_type )
;

field_def_list : field_def ( COMMA! field_def )*
;

// we have two tuple types as implicit_tuple_types can be confused with parentheses around
// a field_def - so to remove this ambiguity we'll decide brackets around a single field_def
// type is *not* a tuple
as_clause : AS^ ( explicit_field_def | ( LEFT_PAREN! field_def_list? RIGHT_PAREN! ) )
;

// OTHERS

stream_cmd_list : stream_cmd ( COMMA stream_cmd )* -> stream_cmd+
;

stream_cmd : ( STDIN | STDOUT | QUOTEDSTRING )^ ( USING! func_clause )?
;

cmd : EXECCOMMAND^ ( ship_clause | cache_clause | input_clause | output_clause | error_clause )*
;

rel : identifier_plus | previous_rel | nested_op_clause
;

previous_rel : ARROBA
;

store_clause : STORE^ rel INTO! QUOTEDSTRING ( USING! func_clause )?
;

assert_clause : ASSERT^ rel BY! cond ( COMMA! QUOTEDSTRING )?
;

filter_clause : FILTER^ rel BY! cond
;

stream_clause : STREAM^ rel THROUGH! ( EXECCOMMAND | identifier_plus ) as_clause?
;

mr_clause : MAPREDUCE^ QUOTEDSTRING ( LEFT_PAREN! path_list RIGHT_PAREN! )? store_clause load_clause EXECCOMMAND?
;

split_clause : SPLIT^ rel INTO! split_branch split_branches
;

split_branch : identifier_plus IF cond -> ^( SPLIT_BRANCH identifier_plus cond )
;

split_otherwise : identifier_plus OTHERWISE^
;

split_branches : COMMA! split_branch split_branches?
               | COMMA! split_otherwise
;

limit_clause : LIMIT^ rel expr
;

sample_clause : SAMPLE^ rel expr
;

rank_clause : RANK^ rel ( rank_by_statement )?
;

rank_by_statement : BY^ rank_by_clause DENSE?
;

rank_by_clause : STAR ( ASC | DESC )?
               | rank_list
;

rank_list : rank_col ( COMMA rank_col )*
         -> rank_col+
;

rank_col : col_range ( ASC | DESC )?
         | col_ref ( ASC | DESC )?
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

rel_list : rel ( COMMA rel )* -> rel+
;

cross_clause : CROSS^ rel_list partition_clause?
;


join_clause : JOIN^ join_sub_clause ( USING! join_type )? partition_clause?
;

join_type : QUOTEDSTRING
;

join_sub_clause : join_item ( ( ( LEFT | RIGHT | FULL ) OUTER? COMMA! join_item ) | ( ( COMMA! join_item )+ ) )
;

join_item : rel join_group_by_clause -> ^( JOIN_ITEM  rel join_group_by_clause )
;

// this can either be a single arg or something like (a,b) - which is
// indistinguishable from a tuple. We'll therefore parse a single argument
// (which can be a tuple of several real_args) and expand it:
join_group_by_clause
    @after
    {
        Tree by = (Tree) retval.getTree();
        Tree realArg = by.getChild(0);
        if(realArg.getType() == TUPLE_VAL
        || (realArg.getType() == FUNC_EVAL && realArg.getChild(0).getType() == TOTUPLE)) {
            retval.tree = adaptor.create(by.getType(), by.getText());
            for(int i = 0; i < realArg.getChildCount(); ++i) {
                if(realArg.getChild(i).getType()!=TOTUPLE)
                ((Tree)retval.tree).addChild(realArg.getChild(i));
            }
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
        }
    }
                     : BY^ real_arg
;

union_clause : UNION^ ONSCHEMA? rel_list
;

cube_clause : CUBE rel BY cube_rollup_list ( COMMA cube_rollup_list )* -> ^( CUBE rel ^( BY cube_rollup_list+ ) )
;

cube_rollup_list : ( CUBE | ROLLUP )^ LEFT_PAREN! real_arg ( COMMA! real_arg )* RIGHT_PAREN!
;

flatten_clause : FLATTEN^ LEFT_PAREN! expr RIGHT_PAREN!
;

// unlike loading and streaming, we want the as_clause (if present) in a different format (i.e.
// we drop the AS token itself).
generate_as_clause :  AS! ( ( LEFT_PAREN! field_def_list RIGHT_PAREN! ) | explicit_field_def )
;

flatten_generated_item : flatten_clause generate_as_clause?
                       | real_arg generate_as_clause?
;

// EXPRESSIONS

// conditional precedence is OR weakest, then AND, then NOT, then IS NOT NULL and the comparison operators equally
// by design the boolean operator hierarchy is entirely below the expression hierarchy

real_arg : expr
         | STAR
         | col_range
;

cond : and_cond  ( OR^ and_cond )*
;

and_cond : not_cond ( AND^ not_cond )*
;

not_cond : NOT^? unary_cond
;

unary_cond
    @after
    {
        // Expressions in parentheses are a little tricky to match as
        // they could contain either "cond" rules or "expr" rules. If
        // they are "expr" rules then they're put under a BOOL_COND node
        // in the tree, but "cond" rules put no extra tokens in the tree.
        // As we're matching non-recursively we'll parse whatever's in the
        // brackets, and if the AST has a boolean expression at its root
        // then we'll assume we've just got a "cond" expression in
        // brackets, and otherwise we'll assume its an "expr" (and so
        // we'll have to strip off the BOOL_COND token the "cast_expr"
        // rule added)
        BaseTree tree = (BaseTree) retval.getTree();
        if(tree.getType() == BOOL_COND
        && tree.getChild(0).getType() == EXPR_IN_PAREN
        && BOOLEAN_TOKENS.contains(tree.getChild(0).getChild(0).getType())) {
            retval.tree = tree.getChild(0).getChild(0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
        }

        // For IN expression, we clone the lhs expression (1st child of the
        // returned tree) and insert it before every rhs expression. For example,
        //
        //   lhs IN (rhs1, rhs2, rhs3)
        // =>
        //   ^( IN lhs, rhs1, lhs, rhs2, lhs, rhs3 )
        //
        // Note that lhs appears three times at index 0, 2 and 4.
        //
        // This is needed because in LogicalPlanGenerator.g, we translate this
        // tree to nested or expressions, and we need to construct a new
        // LogicalExpression object per rhs expression.
        if(tree.getType() == IN) {
            Tree lhs = tree.getChild(0);
            for(int i = 2; i < tree.getChildCount(); i = i + 2) {
                tree.insertChild(i, deepCopy(lhs));
            }
        }
    }
    : exp1 = expr
        ( ( IS NOT? NULL -> ^( NULL $exp1 NOT? ) )
        | ( IN LEFT_PAREN ( rhs_operand ( COMMA rhs_operand )* ) RIGHT_PAREN -> ^( IN ^( IN_LHS expr ) ^( IN_RHS rhs_operand )+ ) )
        | ( rel_op exp2 = expr -> ^( rel_op $exp1 $exp2 ) )
        | ( -> ^(BOOL_COND expr) ) )
;

rhs_operand : expr
;

expr : multi_expr ( ( PLUS | MINUS )^ multi_expr )*
;

multi_expr : cast_expr ( ( STAR | DIV | PERCENT )^ cast_expr )*
;

func_name_suffix : ( ( DOLLAR | PERIOD ) eid )+
;

cast_expr
    @after
    {
        BaseTree tree = (BaseTree) retval.getTree();

        // the parser does an initial optimisation step: it removes TOTUPLE / TOMAP / TOBAG
        // function calls if it knows they'll just return the input (i.e. because the function's
        // argument is a literal). We'll do this here by post-processing the result:
        if(tree.getType() == FUNC_EVAL) {
            Integer func = FUNC_TO_LITERAL.get(tree.getChild(0).getType());
            if(func != null) {
                boolean canBeOptimised = true;
                for(int arg = 1; arg < tree.getChildCount() && canBeOptimised; ++arg) {
                    canBeOptimised &= LITERAL_TOKENS.contains(tree.getChild(arg).getType());
                }
                if(canBeOptimised) {
                    retval.tree = adaptor.create(func, func.toString());
                    ((BaseTree)retval.tree).addChildren(tree.getChildren());
                    ((BaseTree)retval.tree).deleteChild(0); // the (e.g.) TOBAG token
                    adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
                }
            }
        }

        // a minor correction to the token text for formatting -
        // we want NEG's text to be the same as MINUSes
        if(tree.getType() == NEG) {
            ((CommonTree)tree).token.setText("-");
        }

        // As noted below, brackets around a single literal mean a tuple
        // of that literal, not a nested expression which evaluates to
        // that literal. Remember that a NULL with children is a boolean
        // expression, not a literal!
        if(tree.getType() == EXPR_IN_PAREN
        && LITERAL_TOKENS.contains(tree.getChild(0).getType())
        && (tree.getChild(0).getType() != NULL || tree.getChild(0).getChildCount() == 0)) {
            ((CommonTree)tree).token.setType(TUPLE_VAL);
        }

        // For CASE statement, we clone the case expression (1st child of the
        // returned tree) and insert it before every when expression. For example,
        //
        //   CASE e1
        //     WHEN e2 THEN e3
        //     WHEN e4 THEN e5
        //     ELSE e6
        //   END
        // =>
        //   ^( CASE e1, e2, e3, e1, e4, e5, e6 )
        //
        // Note that e1 appears twice at index 0 and 3.
        //
        // This is needed because in LogicalPlanGenerator.g, we translate this
        // tree to nested bincond expressions, and we need to construct a new
        // LogicalExpression object per when branch.
        if(tree.getType() == CASE_EXPR) {
            Tree caseExpr = tree.getChild(0);
            int childCount = tree.getChildCount();
            boolean hasElse = childCount \% 2 == 0;
            int whenBranchCount = ( childCount - (hasElse ? 2 : 1) ) / 2;
            for(int i = 1; i < whenBranchCount; i++) {
                tree.insertChild(3*i, deepCopy(caseExpr));
            }
        }
    }
          : scalar
          | MINUS cast_expr -> ^( NEG cast_expr )
          // single columns and functions (both of which can start with an identifier). Note that we have to be
          // careful with periods straight after the identifier, as we want those to be projections, not function
          // calls
          | col_ref_without_identifier projection*
          | invoker_func projection*
          | identifier_plus projection*
          | identifier_plus func_name_suffix? LEFT_PAREN ( real_arg ( COMMA real_arg )* )? RIGHT_PAREN projection* -> ^( FUNC_EVAL identifier_plus func_name_suffix? real_arg* ) projection*
          | func_name_without_columns LEFT_PAREN ( real_arg ( COMMA real_arg )* )? RIGHT_PAREN projection* -> ^( FUNC_EVAL func_name_without_columns real_arg* ) projection*
          | CASE ( (WHEN)=> WHEN cond THEN expr ( WHEN cond THEN expr )* ( ELSE expr )? END projection* -> ^( CASE_COND ^(WHEN cond+) ^(THEN expr+) ) projection*
                 | expr WHEN rhs_operand THEN rhs_operand ( WHEN rhs_operand THEN rhs_operand )* ( ELSE rhs_operand )? END projection*
                 -> ^( CASE_EXPR ^(CASE_EXPR_LHS expr) ^(CASE_EXPR_RHS rhs_operand)+ ) projection*
                 )
          | paren_expr
          | curly_expr
          | bracket_expr
;

invoker_func
@init {
    String staticStr = "true";
    List<String> packageStr = Lists.newArrayList();
    String methodStr = null;
}
: INVOKE ( AMPERSAND | LEFT_PAREN real_arg { staticStr = "false"; } RIGHT_PAREN ) ( packageName=identifier_plus PERIOD { packageStr.add($packageName.text); } )* methodName=identifier_plus { methodStr=$methodName.text; } LEFT_PAREN ( real_arg ( COMMA real_arg )* )? RIGHT_PAREN
              -> ^( INVOKER_FUNC_EVAL IDENTIFIER[Joiner.on(".").join(packageStr)] IDENTIFIER[methodStr] IDENTIFIER[staticStr] real_arg* )
;

// now we have to deal with parentheses: in an expr, '(' can be the
// start of a cast, the start of a nested expression or the start of
// a tuple. We'll ensure parsing is unambiguous by assuming a single
// expression in parentheses is a nested expression, whereas two or
// more nested expressions are a tuple (unless that single expression
// is a literal, in which case we assume tuple with a single element
// - that literal).
paren_expr
    @after
    {
        BaseTree tree = (BaseTree)retval.getTree();

        // the other side of the @after block in unary_cond: if we've
        // matched an EXPR_IN_PAREN we expect the nested expression to
        // be an "expr", not a "cond", so we should strip off the
        // BOOL_COND token.
        if(tree.getType() == EXPR_IN_PAREN
        && tree.getChild(0).getType() == BOOL_COND) {
            int type = tree.getChild(0).getChild(0).getType();
            // NULL is a special case - if it has children it's a boolean
            // expression, and if not it's a literal NULL. Note that we
            // replace *all* children
            if(!BOOLEAN_TOKENS.contains(type)
            || (type == NULL && tree.getChild(0).getChild(0).getChildCount() == 0)) {
                Tree addChildrenOf = tree.getChild(0);
                for(int i = 0; i < tree.getChildCount(); ++i)
                    tree.deleteChild(i);
                for(int i = 0; i < addChildrenOf.getChildCount(); ++i)
                    tree.addChild(addChildrenOf.getChild(i));
            }
        }

        // A function call to TOTUPLE is inserted into the AST for
        // some tuple literals - but as we assume the first expression
        // after an open bracket is a "cond" rule, and as "cond" rules
        // nest "expr" rules under a BOOL_COND token we get an invalid
        // AST. We'll remove this BOOL_COND here:
        if(tree.getType() == FUNC_EVAL
        && tree.getChild(0).getType() == TOTUPLE
        && tree.getChildCount() > 1
        && tree.getChild(1).getType() == BOOL_COND) {
            Tree insertChildrenOf = tree.getChild(1);
            tree.deleteChild(1);
            for(int i = insertChildrenOf.getChildCount() - 1; i >= 0; --i)
                tree.insertChild(1, insertChildrenOf.getChild(i));
        }
    }
    : LEFT_PAREN! try_implicit_map_cast
;

try_implicit_map_cast
           // we'll also allow implicit map casts (for backwards compatibility only -
           // bag and tuple casts have to be explicit and it makes the grammar more
           // simple). Unfortunately we'll have to turn on back-tracking for this rule,
           // as LEFT_PAREN LEFT_BRACKET could be a literal map in a EXPR_IN_PAREN.
           // It'd be much better if we could remove this from the Pig language (and
           // just rely on explicit map casts) - then we'd have no backtracking at all!
           : ( implicit_map_type RIGHT_PAREN cast_expr) => implicit_map_type RIGHT_PAREN cast_expr -> ^( CAST_EXPR implicit_map_type cast_expr )
           | after_left_paren
;

after_left_paren : explicit_type_cast RIGHT_PAREN cast_expr -> ^( CAST_EXPR explicit_type_cast cast_expr )
                 // tuples
                 | RIGHT_PAREN projection* -> ^( TUPLE_VAL ) projection*
                 | STAR ( COMMA real_arg )* RIGHT_PAREN projection* -> ^( FUNC_EVAL TOTUPLE STAR real_arg* ) projection*
                 | col_range ( COMMA real_arg )* RIGHT_PAREN projection* -> ^( FUNC_EVAL TOTUPLE col_range real_arg* ) projection*
                 // Tuples begin with '(' expr, but shorthand-booleans begin with '(' cond. As cond
                 // and expr are indistinguishable, we'll parse as a cond (i.e. the most lenient) and
                 // for exprs, strip off the BOOL_COND trees. You can have both nested conds and nested
                 // exprs, so we'll just assume cond.
                 | cond
                   ( ( ( COMMA real_arg )+ RIGHT_PAREN projection* -> ^( FUNC_EVAL TOTUPLE cond real_arg+ ) projection* )
                   | ( RIGHT_PAREN -> ^( EXPR_IN_PAREN cond ) )
                   | ( QMARK exp1 = expr COLON exp2 = expr RIGHT_PAREN -> ^( BIN_EXPR cond $exp1 $exp2 ) ) )
;

curly_expr : LEFT_CURLY real_arg ( COMMA real_arg )* RIGHT_CURLY projection* -> ^( FUNC_EVAL TOBAG real_arg+ ) projection*
           | LEFT_CURLY RIGHT_CURLY projection* -> ^( BAG_VAL ) projection*
;

bracket_expr : LEFT_BRACKET real_arg ( COMMA real_arg )* RIGHT_BRACKET projection* -> ^( FUNC_EVAL TOMAP real_arg+ ) projection*
             | LEFT_BRACKET keyvalue ( COMMA keyvalue )* RIGHT_BRACKET projection* -> ^( MAP_VAL keyvalue+ ) projection*
             | LEFT_BRACKET RIGHT_BRACKET projection* -> ^( MAP_VAL ) projection*
;

projection : PERIOD ( col_ref | LEFT_PAREN col_ref ( COMMA col_ref )* RIGHT_PAREN ) -> ^( PERIOD col_ref+ )
           | POUND^ ( QUOTEDSTRING | NULL )
;

// ATOMS

// for disambiguation with func_names
col_ref_without_identifier : GROUP | DOLLARVAR
;

col_ref : col_ref_without_identifier | identifier_plus
;

col_range : c1 = col_ref DOUBLE_PERIOD c2 = col_ref? -> ^(COL_RANGE $c1 DOUBLE_PERIOD $c2?)
          |  DOUBLE_PERIOD col_ref -> ^(COL_RANGE DOUBLE_PERIOD col_ref)
;

scalar : INTEGER
       | LONGINTEGER
       | FLOATNUMBER
       | DOUBLENUMBER
       | QUOTEDSTRING
       | NULL
       | TRUE
       | FALSE
;

keyvalue : QUOTEDSTRING POUND literal -> ^( KEY_VAL_PAIR QUOTEDSTRING literal )
;

literal_map : LEFT_BRACKET keyvalue ( COMMA keyvalue )* RIGHT_BRACKET -> ^( MAP_VAL keyvalue+ )
            | LEFT_BRACKET RIGHT_BRACKET -> ^( MAP_VAL )
;


literal_bag : LEFT_CURLY literal_tuple ( COMMA literal_tuple )* RIGHT_CURLY -> ^( BAG_VAL literal_tuple+ )
            | LEFT_CURLY RIGHT_CURLY -> ^( BAG_VAL )
;

literal_tuple : LEFT_PAREN literal ( COMMA literal )* RIGHT_PAREN -> ^( TUPLE_VAL literal+ )
              | LEFT_PAREN RIGHT_PAREN -> ^( TUPLE_VAL )
;

literal : scalar | literal_map | literal_bag | literal_tuple
;

// NESTING

nested_blk : ( nested_command SEMI_COLON )* GENERATE flatten_generated_item ( COMMA flatten_generated_item )* SEMI_COLON
    -> nested_command* ^( GENERATE flatten_generated_item+ )
;

nested_command : ( identifier_plus EQUAL col_ref PERIOD col_ref_list { input.LA( 1 ) == SEMI_COLON }? ) => ( identifier_plus EQUAL nested_proj )
              -> ^( NESTED_CMD identifier_plus nested_proj )
               | identifier_plus EQUAL expr
              -> ^( NESTED_CMD_ASSI identifier_plus expr )
               | identifier_plus EQUAL nested_op
              -> ^( NESTED_CMD identifier_plus nested_op )
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

nested_foreach: FOREACH nested_op_input GENERATE flatten_generated_item ( COMMA flatten_generated_item )*
    -> ^( FOREACH nested_op_input ^( GENERATE flatten_generated_item+ ) )
;

nested_op_input : col_ref | nested_proj
;

nested_op_input_list : nested_op_input ( COMMA nested_op_input )*
        -> nested_op_input+
;

// IDENTIFIERS

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid_without_columns : rel_str_op
    | IMPORT
    | REGISTER
    | RETURNS
    | DEFINE
    | LOAD
    | FILTER
    | FOREACH
    | ROLLUP
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
    | AND
    | OR
    | GENERATE
    | ASC
    | DESC
    | BOOL
    | BIGINTEGER
    | BIGDECIMAL
    | DATETIME
    | CHARARRAY
    | BYTEARRAY
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
    | REALIAS
    | BOOL_COND
    | ASSERT
;

eid : eid_without_columns
    | IDENTIFIER
    | GROUP
    | CUBE
    | TRUE
    | FALSE
    | INT
    | LONG
    | FLOAT
    | DOUBLE
    | NULL
    | NOT
    | FLATTEN
    | BAG
    | TUPLE
    | MAP
;

// relational operator
rel_op : rel_str_op
       | NUM_OP_EQ
       | NUM_OP_NE
       | NUM_OP_GT
       | NUM_OP_GTE
       | NUM_OP_LT
       | NUM_OP_LTE
;

rel_str_op : STR_OP_EQ
           | STR_OP_NE
           | STR_OP_GT
           | STR_OP_LT
           | STR_OP_GTE
           | STR_OP_LTE
           | STR_OP_MATCHES
;

reserved_identifier_whitelist : RANK
                              | CUBE
                              | IN
                              | WHEN
                              | THEN
                              | ELSE
                              | END
;

