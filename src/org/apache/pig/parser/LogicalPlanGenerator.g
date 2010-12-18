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
 * Grammar file for Pig tree parser (for schema alias validation).
 *
 * NOTE: THIS FILE IS BASED ON QueryParser.g, SO IF YOU CHANGE THAT FILE, YOU WILL 
 *       PROBABLY NEED TO MAKE CORRESPONDING CHANGES TO THIS FILE AS WELL.
 */

tree grammar LogicalPlanGenerator;

options {
    tokenVocab=QueryParser;
    ASTLabelType=CommonTree;
    output=AST;
    backtrack=true;
}

@header {
package org.apache.pig.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
}

@members {
private static Log log = LogFactory.getLog( LogicalPlanGenerator.class );

private LogicalPlanBuilder builder = new LogicalPlanBuilder();

public LogicalPlan getLogicalPlan() {
    return builder.getPlan();
}
} // End of @members

query : ^( QUERY statement* )
;

statement : general_statement | foreach_statement
;

general_statement 
scope {
    String alias;
    Integer parallel;
}
@init {
}
: ^( STATEMENT ( alias { $general_statement::alias = $alias.name; } )? 
  op_clause ( INTEGER { $general_statement::parallel = Integer.parseInt( $INTEGER.text ); } )? )
;

// We need to handle foreach specifically because of the ending ';', which is not required 
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : ^( STATEMENT alias? foreach_clause[$alias.text] )
;

alias returns[String name]: IDENTIFIER { $name = $IDENTIFIER.text; }
;

op_clause returns[String alias] : 
            define_clause 
          | load_clause { $alias = $load_clause.alias; }
          | group_clause { $alias = $group_clause.alias; }
          | store_clause { $alias = $store_clause.alias; }
          | filter_clause { $alias = $filter_clause.alias; }
          | distinct_clause { $alias = $distinct_clause.alias; }
          | limit_clause { $alias = $limit_clause.alias; }
          | sample_clause { $alias = $sample_clause.alias; }
          | order_clause { $alias = $order_clause.alias; }
          | cross_clause { $alias = $cross_clause.alias; }
          | join_clause { $alias = $join_clause.alias; }
          | union_clause { $alias = $union_clause.alias; }
          | stream_clause
          | mr_clause
          | split_clause { $alias = $split_clause.alias; }
;

define_clause 
 : ^( DEFINE alias cmd ) 
 | ^( DEFINE alias func_clause )
   {
       builder.defineFunction( $alias.name, $func_clause.funcSpec );
   }
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

load_clause returns[String alias]
 : ^( LOAD filename func_clause? as_clause? )
  {
      $alias = builder.buildLoadOp( $general_statement::alias,
          $general_statement::parallel, $filename.filename, $func_clause.funcSpec, $as_clause.logicalSchema  );
  }
;

filename returns[String filename]
 : QUOTEDSTRING { $filename = builder.unquote( $QUOTEDSTRING.text ); }
;

as_clause returns[LogicalSchema logicalSchema]
 : ^( AS tuple_def ) { $logicalSchema = $tuple_def.logicalSchema; }
;

tuple_def returns[LogicalSchema logicalSchema]
 : tuple_def_full { $logicalSchema = $tuple_def_full.logicalSchema; }
 | tuple_def_simple { $logicalSchema = $tuple_def_simple.logicalSchema; }
;

tuple_def_full returns[LogicalSchema logicalSchema]
@init { $logicalSchema = new LogicalSchema(); }
 : ^( TUPLE_DEF ( field { $logicalSchema.addField( $field.fieldSchema ); } )+ ) 
;

tuple_def_simple returns[LogicalSchema logicalSchema]
 : ^( TUPLE_DEF field )
   {
       $logicalSchema = new LogicalSchema();
       $logicalSchema.addField( $field.fieldSchema );
   }
;

field returns[LogicalFieldSchema fieldSchema]
 : ^( FIELD IDENTIFIER type )
   {
       $fieldSchema = new LogicalFieldSchema( $IDENTIFIER.text, $type.logicalSchema, $type.datatype );
   }
;

type returns[byte datatype, LogicalSchema logicalSchema]
 : simple_type
   {
        $datatype = $simple_type.datatype;
   }
 | tuple_type
   {
       $datatype = DataType.TUPLE;
       $logicalSchema = $tuple_type.logicalSchema;
   }
 | bag_type
   {
       $datatype = DataType.BAG;
       $logicalSchema = $bag_type.logicalSchema;
   }
 | map_type
   {
       $datatype = DataType.MAP;
   }
;

simple_type returns[byte datatype]
 : INT { $datatype = DataType.INTEGER; }
 | LONG { $datatype = DataType.LONG; }
 | FLOAT { $datatype = DataType.FLOAT; }
 | DOUBLE { $datatype = DataType.DOUBLE; }
 | CHARARRAY { $datatype = DataType.CHARARRAY; }
 | BYTEARRAY { $datatype = DataType.BYTEARRAY; }
;

tuple_type returns[LogicalSchema logicalSchema]
 : ^( TUPLE_TYPE tuple_def_full )
   { 
       $logicalSchema = $tuple_def_full.logicalSchema;
   }
;

bag_type returns[LogicalSchema logicalSchema]
 : ^( BAG_TYPE tuple_def? )
   { 
       $logicalSchema = $tuple_def.logicalSchema;
   }
;

map_type : MAP_TYPE
;

func_clause returns[FuncSpec funcSpec]
 : ^( FUNC func_name func_args? )
   { 
       $funcSpec = builder.buildFuncSpec( $func_name.funcName, $func_args.args );
   }
 | ^( FUNC func_alias )
   {
       $funcSpec = builder.lookupFunction( $func_alias.alias );
   }
;

func_name returns[String funcName]
@init { StringBuilder buf = new StringBuilder(); } 
 : p1 = eid { buf.append( $p1.id ); }
      ( ( PERIOD { buf.append( $PERIOD.text ); } | DOLLAR { buf.append( $PERIOD.text ); } )
      p2 = eid { buf.append( $p2.id ); } )*
   {
       $funcName = buf.toString();
   }
;

func_alias returns[String alias]
 : IDENTIFIER
   {
       $alias = $IDENTIFIER.text;
   }
;

func_args returns[List<String> args]
@init { $args = new ArrayList<String>(); }
: ( QUOTEDSTRING { $args.add( builder.unquote( $QUOTEDSTRING.text ) ); } )+
;

group_clause returns[String alias]
 : ^( GROUP group_item_list QUOTEDSTRING? )
 | ^( COGROUP group_item_list QUOTEDSTRING? )
;

group_item_list : group_item+
;

group_item : rel ( join_group_by_clause[$rel.name] | ALL | ANY ) ( INNER | OUTER )?
;

rel returns[String name] : alias { $name = $alias.name; }
                        | op_clause { $name = $op_clause.alias; }
;

flatten_generated_item
 : ( flatten_clause | expr[null] | STAR ) as_clause?
;

flatten_clause
 : ^( FLATTEN expr[null] )
;

store_clause returns[String alias]
 : ^( STORE alias filename func_clause? )
   {
       $alias= builder.buildStoreOp( $general_statement::alias,
          $general_statement::parallel, $alias.name, $filename.filename, $func_clause.funcSpec );
   }
;

filter_clause returns[String alias]
@init { LogicalExpressionPlan exprPlan = new LogicalExpressionPlan(); }
 : ^( FILTER rel cond[exprPlan] )
   {
       $alias = builder.buildFilterOp( $general_statement::alias,
          $general_statement::parallel, $rel.name, exprPlan );
   }
;

cond[LogicalExpressionPlan exprPlan] returns[LogicalExpression expr]
 : ^( OR left = cond[exprPlan] right = cond[exprPlan] )
   {
       $expr = new OrExpression( $exprPlan, $left.expr, $right.expr );
   }
 | ^( AND left = cond[exprPlan] right = cond[exprPlan] )
   {
       $expr = new AndExpression( $exprPlan, $left.expr, $right.expr );
   }
 | ^( NOT c = cond[exprPlan] )
   {
       $expr = new NotExpression( $exprPlan, $c.expr );
   }
 | ^( NULL expr[$exprPlan] NOT? )
   {
       $expr = new IsNullExpression( $exprPlan, $expr.expr );
       if( $NOT != null )
           $expr = new NotExpression( $exprPlan, $expr );
   }
 | ^( rel_op_eq e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new EqualExpression( $exprPlan, $e1.expr, $e2.expr );
   } 
 | ^( rel_op_ne e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new NotEqualExpression( $exprPlan, $e1.expr, $e2.expr );
   } 
 | ^( rel_op_lt e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new LessThanExpression( $exprPlan, $e1.expr, $e2.expr );
   } 
 | ^( rel_op_lte e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new LessThanEqualExpression( $exprPlan, $e1.expr, $e2.expr );
   }
 | ^( rel_op_gt e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new GreaterThanExpression( $exprPlan, $e1.expr, $e2.expr );
   } 
 | ^( rel_op_gte e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new GreaterThanEqualExpression( $exprPlan, $e1.expr, $e2.expr );
   }
 | ^( STR_OP_MATCHES e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new RegexExpression( $exprPlan, $e1.expr, $e2.expr );
   }
 | func_eval[$exprPlan]
;

func_eval[LogicalExpressionPlan plan] returns[LogicalExpression expr]
@init { List<LogicalExpression> args = new ArrayList<LogicalExpression>(); }
 : ^( FUNC_EVAL func_name ( real_arg[$plan] { args.add( $real_arg.expr ); } )* )
   {
       //TODO: FuncSpec funcSpec = builder.buildFuncSpec( $func_name.funcName, $func_args.args );
   }
;

real_arg [LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : e = expr[$plan] { $expr = $e.expr; }
 | STAR
;

expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( PLUS left = expr[$plan] right = expr[$plan] )
   {
       $expr = new AddExpression( $plan, $left.expr, $right.expr );
   }
 | ^( MINUS left = expr[$plan] right = expr[$plan] )
   {
       $expr = new SubtractExpression( $plan, $left.expr, $right.expr );
   }
 | ^( STAR left = expr[$plan] right = expr[$plan] )
   {
       $expr = new MultiplyExpression( $plan, $left.expr, $right.expr );
   }
 | ^( DIV left = expr[$plan] right = expr[$plan] )
   {
       $expr = new DivideExpression( $plan, $left.expr, $right.expr );
   }
 | ^( PERCENT left = expr[$plan] right = expr[$plan] )
   {
       $expr = new ModExpression( $plan, $left.expr, $right.expr );
   }
 | const_expr[$plan]
   {
       $expr = new ConstantExpression( $plan, $const_expr.expr, null );
   }
 | var_expr[$plan]
   {
       $expr = $var_expr.expr; 
   }
 | ^( NEG e = expr[$plan] )
   {
       $expr = new NegativeExpression( $plan, $e.expr );
   }
 | ^( CAST_EXPR type e = expr[$plan] ) // cast expr
   {
       $expr = new CastExpression( $plan, $e.expr, 
           new LogicalFieldSchema( null , $type.logicalSchema, $type.datatype ) );
   }
 | ^( EXPR_IN_PAREN e = expr[$plan] ) // unary expr
   {
       $expr = $e.expr;
   }
;

var_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : projectable_expr[$plan] ( dot_proj | pound_proj )*
;

projectable_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : func_eval[$plan] { $expr = $func_eval.expr; }
 | col_ref[$plan] { }
 | bin_expr[$plan] { $expr = $bin_expr.expr; }
;

dot_proj : ^( PERIOD col_ref[null]+ )
;

pound_proj : ^( POUND ( QUOTEDSTRING | NULL ) )
;

bin_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( BIN_EXPR cond[$plan] e1 = expr[$plan] e2 = expr[$plan] )
   {
       $expr = new BinCondExpression( $plan, $cond.expr, $e1.expr, $e2.expr );
   }
;

limit_clause returns[String alias]
 : ^( LIMIT rel INTEGER  )
   {
       $alias = builder.buildLimitOp( $general_statement::alias,
           $general_statement::parallel, $rel.name, Long.valueOf( $INTEGER.text ) );
   }
 | ^( LIMIT rel LONGINTEGER )
   {
       $alias = builder.buildLimitOp( $general_statement::alias,
           $general_statement::parallel, $rel.name, Long.valueOf( $LONGINTEGER.text ) );
   }
;

sample_clause returns[String alias]
 : ^( SAMPLE rel DOUBLENUMBER )
   {
       $alias = builder.buildSampleOp( $general_statement::alias,
           $general_statement::parallel, $rel.name, Double.valueOf( $DOUBLENUMBER.text ) );
   }
;

order_clause returns[String alias]
 : ^( ORDER rel order_by_clause[$rel.name] func_clause? )
   {
       $alias = builder.buildOrderOp( $general_statement::alias,
           $general_statement::parallel, $rel.name, $order_by_clause.plans, 
           $order_by_clause.ascFlags, $func_clause.funcSpec );
   }
;

order_by_clause[String opAlias] returns[List<LogicalExpressionPlan> plans, List<Boolean> ascFlags]
@init {
    $plans = new ArrayList<LogicalExpressionPlan>();
    $ascFlags = new ArrayList<Boolean>();
}
 : STAR {
       LogicalExpressionPlan plan = builder.buildProjectStar( $opAlias );
       $plans.add( plan );
   }
   ( ASC | DESC { $ascFlags.add( false ); } )?
 | ( order_col
   {
       $plans.add( $order_col.plan );
       $ascFlags.add( $order_col.ascFlag );
   } )+
;

order_col returns[LogicalExpressionPlan plan, Boolean ascFlag]
@init { 
    $plan = new LogicalExpressionPlan();
    $ascFlag = true;
}
 : col_ref[$plan] ( ASC | DESC { $ascFlag = false; } )?
;

distinct_clause returns[String alias]
 : ^( DISTINCT rel partition_clause? )
   {
       $alias = builder.buildDistinctOp( $general_statement::alias,
          $general_statement::parallel, $rel.name, $partition_clause.partitioner );
   }
;

partition_clause returns[String partitioner]
 : ^( PARTITION func_name )
   {
       $partitioner = $func_name.funcName;
   }
;

cross_clause returns[String alias]
 : ^( CROSS rel_list partition_clause? )
   {
       $alias = builder.buildCrossOp( $general_statement::alias,
          $general_statement::parallel, $rel_list.aliasList, $partition_clause.partitioner );
   }
;

rel_list returns[List<String> aliasList]
@init { $aliasList = new ArrayList<String>(); }
 : ( rel { $aliasList.add( $rel.name ); } )+
;

join_clause returns[String alias]
scope {
    MultiMap<Integer, LogicalExpressionPlan> joinPlans;
    int inputIndex;
    List<String> inputAliases;
    List<Boolean> innerFlags;
}
@init {
    $join_clause::joinPlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $join_clause::inputAliases = new ArrayList<String>();
    $join_clause::innerFlags = new ArrayList<Boolean>();
}
 : ^( JOIN join_sub_clause join_type? partition_clause? )
   {
       $alias = builder.buildJoinOp( $general_statement::alias,
          $general_statement::parallel, $join_clause::inputAliases, $join_clause::joinPlans,
          $join_type.type, $join_clause::innerFlags, $partition_clause.partitioner );
   }
;

join_type returns[JOINTYPE type]
 : JOIN_TYPE_REPL { $type = JOINTYPE.REPLICATED; }
 | JOIN_TYPE_MERGE { $type = JOINTYPE.MERGE; }
 | JOIN_TYPE_SKEWED { $type = JOINTYPE.SKEWED; }
 | JOIN_TYPE_DEFAULT { $type = JOINTYPE.HASH; }
;

join_sub_clause
 : join_item ( LEFT { $join_clause::innerFlags.add( false ); 
                      $join_clause::innerFlags.add( true ); } 
             | RIGHT { $join_clause::innerFlags.add( true ); 
                       $join_clause::innerFlags.add( false ); }
             | FULL { $join_clause::innerFlags.add( false ); 
                      $join_clause::innerFlags.add( false ); } ) OUTER? join_item
   {
   }
 | join_item+
;

join_item
 : ^( JOIN_ITEM rel join_group_by_clause[$rel.name] )
   {
       $join_clause::inputAliases.add( $rel.name );
       $join_clause::joinPlans.put( $join_clause::inputIndex, $join_group_by_clause.plans );
       $join_clause::inputIndex++;
   }
;

join_group_by_clause[String alias] returns[List<LogicalExpressionPlan> plans]
scope { String inputAlias; }
@init {
    $join_group_by_clause::inputAlias = $alias;
    $plans = new ArrayList<LogicalExpressionPlan>();
}
 : ^( BY ( join_group_by_expr { $plans.add( $join_group_by_expr.plan ); } )+ )
;

join_group_by_expr returns[LogicalExpressionPlan plan]
 : { $plan = new LogicalExpressionPlan(); } expr[$plan]
 | STAR 
   {
       $plan = builder.buildProjectStar( $join_group_by_clause::inputAlias );
   }
;

union_clause returns[String alias]
 : ^( UNION ONSCHEMA? rel_list )
   {
      $alias = builder.buildUnionOp( $general_statement::alias,
          $general_statement::parallel, $rel_list.aliasList );
   }
;

foreach_clause[String alias] returns[LogicalRelationalOperator op] : ^( FOREACH rel nested_plan )
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

nested_command : ^( NESTED_CMD IDENTIFIER ( expr[null] | nested_op ) )
;

nested_op : nested_proj
          | nested_filter
          | nested_sort
          | nested_distinct
          | nested_limit
;

nested_proj : ^( NESTED_PROJ col_ref[null] col_ref_list )
;

col_ref_list : col_ref[null]+
;

nested_alias_ref : IDENTIFIER
;

nested_filter : ^( FILTER ( nested_alias_ref | nested_proj | expr[null] ) cond[null] )
;

nested_sort : ^( ORDER ( nested_alias_ref | nested_proj | expr[null] )  order_by_clause[null] func_clause? )
;

nested_distinct : ^( DISTINCT ( nested_alias_ref | nested_proj | expr[null] ) )
;

nested_limit : ^( LIMIT ( nested_alias_ref | nested_proj | expr[null] ) INTEGER )
;

stream_clause : ^( STREAM rel ( EXECCOMMAND | IDENTIFIER ) as_clause? )
;

mr_clause : ^( MAPREDUCE QUOTEDSTRING path_list? store_clause load_clause EXECCOMMAND? )
;

split_clause returns[String alias]
 : ^( SPLIT rel { $alias = builder.buildSplitOp( $general_statement::alias, 
                  $general_statement::parallel, $rel.name ); } 
      split_branch[$alias]+ )
   
;

split_branch[String inputAlias] returns[String alias]
@init { LogicalExpressionPlan splitPlan = new LogicalExpressionPlan(); }
 : ^( SPLIT_BRANCH IDENTIFIER cond[splitPlan] )
   {
       $alias = builder.buildSplitOutputOp( $IDENTIFIER.text,
          $general_statement::parallel, $inputAlias, splitPlan );
   }
;

col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : alias_col_ref[$plan] { $expr = $alias_col_ref.expr; }
 | dollar_col_ref[$plan] { $expr = $dollar_col_ref.expr; }
;

alias_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : GROUP 
 | IDENTIFIER
;

dollar_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( DOLLAR INTEGER )
;

const_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : literal
   {
       $expr = new ConstantExpression( $plan, $literal.value,
           new LogicalFieldSchema( null , null, $literal.type ) );
   }
;

literal returns[Object value, byte type]
 : scalar
   {
       $value = $scalar.value;
       $type = $scalar.type;
   }
 | map
   {
       $value = $map.value;
       $type = DataType.MAP;
   }
 | bag
   {
       $value = $bag.value;
       $type = DataType.BAG;
   }
 | tuple
   {
       $value = $tuple.value;
       $type = DataType.TUPLE;
   }
;

scalar returns[Object value, byte type]
 : INTEGER
   { 
       $type = DataType.INTEGER;
       $value = Integer.valueOf( $INTEGER.text );
   }
 | LONGINEGER 
   { 
       $type = DataType.LONG;
       String num = $LONGINEGER.text.substring( 0, $LONGINEGER.text.length() - 1 );
       $value = Long.valueOf( $LONGINEGER.text );
   }
 | FLOATNUMBER 
   { 
       $type = DataType.FLOAT;
       $value = Float.valueOf( $FLOATNUMBER.text );
   }
 | DOUBLENUMBER 
   { 
       $type = DataType.DOUBLE;
       $value = Double.valueOf( $DOUBLENUMBER.text );
   }
 | QUOTEDSTRING 
   { 
       $type = DataType.CHARARRAY;
       $value = builder.unquote( $QUOTEDSTRING.text );
   }
 | NULL
   { 
       $type = DataType.NULL;
   }
;

map returns[Object value]
@init { Map<String, Object> kvs = new HashMap<String, Object>(); }
 : ^( MAP_VAL ( keyvalue { kvs.put( $keyvalue.key, $keyvalue.value ); } )* )
   {
       $value = kvs;
   }
;

keyvalue returns[String key, Object value]
 : ^( KEY_VAL_PAIR map_key literal )
   {
       $key = $map_key.value;
       $value = $literal.value;
   }
;

map_key returns[String value]
 : QUOTEDSTRING { $value = builder.unquote( $QUOTEDSTRING.text ); }
 | NULL { $value = null; }
;

bag returns[Object value]
@init { DataBag dataBag = builder.createDataBag(); }
 : ^( BAG_VAL ( tuple { dataBag.add( $tuple.value ); } )* )
   {
       $value = dataBag;
   }
;

tuple returns[Tuple value]
@init { List<Object> objList = new ArrayList<Object>(); }
 : ^( TUPLE_VAL ( literal { objList.add( $literal.value ); } )* )
   {
       $value = builder.buildTuple( objList );
   }
;

// extended identifier, handling the keyword and identifier conflicts. Ugly but there is no other choice.
eid returns[String id] : rel_str_op { $id = $rel_str_op.id; }
    | DEFINE { $id = $DEFINE.text; }
    | LOAD { $id = $LOAD.text; }
    | FILTER { $id = $FILTER.text; }
    | FOREACH { $id = $FOREACH.text; }
    | MATCHES { $id = $MATCHES.text; }
    | ORDER { $id = $ORDER.text; }
    | DISTINCT { $id = $DISTINCT.text; }
    | COGROUP { $id = $COGROUP.text; }
    | JOIN { $id = $JOIN.text; }
    | CROSS { $id = $CROSS.text; }
    | UNION { $id = $UNION.text; }
    | SPLIT { $id = $SPLIT.text; }
    | INTO { $id = $INTO.text; }
    | IF { $id = $IF.text; }
    | ALL { $id = $ALL.text; }
    | AS { $id = $AS.text; }
    | BY { $id = $BY.text; }
    | USING { $id = $USING.text; }
    | INNER { $id = $INNER.text; }
    | OUTER { $id = $OUTER.text; }
    | PARALLEL { $id = $PARALLEL.text; }
    | PARTITION { $id = $PARTITION.text; }
    | GROUP { $id = $GROUP.text; }
    | AND { $id = $AND.text; }
    | OR { $id = $OR.text; }
    | NOT { $id = $NOT.text; }
    | GENERATE { $id = $GENERATE.text; }
    | FLATTEN { $id = $FLATTEN.text; }
    | EVAL { $id = $EVAL.text; }
    | ASC { $id = $ASC.text; }
    | DESC { $id = $DESC.text; }
    | INT { $id = $INT.text; }
    | LONG { $id = $LONG.text; }
    | FLOAT { $id = $FLOAT.text; }
    | DOUBLE { $id = $DOUBLE.text; }
    | CHARARRAY { $id = $CHARARRAY.text; }
    | BYTEARRAY { $id = $BYTEARRAY.text; }
    | BAG { $id = $BAG.text; }
    | TUPLE { $id = $TUPLE.text; }
    | MAP { $id = $MAP.text; }
    | IS { $id = $IS.text; }
    | NULL { $id = $NULL.text; }
    | STREAM { $id = $STREAM.text; }
    | THROUGH { $id = $THROUGH.text; }
    | STORE { $id = $STORE.text; }
    | MAPREDUCE { $id = $MAPREDUCE.text; }
    | SHIP { $id = $SHIP.text; }
    | CACHE { $id = $CACHE.text; }
    | INPUT { $id = $INPUT.text; }
    | OUTPUT { $id = $OUTPUT.text; }
    | ERROR { $id = $ERROR.text; }
    | STDIN { $id = $STDIN.text; }
    | STDOUT { $id = $STDOUT.text; }
    | LIMIT { $id = $LIMIT.text; }
    | SAMPLE { $id = $SAMPLE.text; }
    | LEFT { $id = $LEFT.text; }
    | RIGHT { $id = $RIGHT.text; }
    | FULL { $id = $FULL.text; }
    | IDENTIFIER { $id = $IDENTIFIER.text; }
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

rel_str_op returns[String id]
 : STR_OP_EQ { $id = $STR_OP_EQ.text; }
 | STR_OP_NE { $id = $STR_OP_NE.text; }
 | STR_OP_GT { $id = $STR_OP_GT.text; }
 | STR_OP_LT { $id = $STR_OP_LT.text; }
 | STR_OP_GTE { $id = $STR_OP_GTE.text; }
 | STR_OP_LTE { $id = $STR_OP_LTE.text; }
 | STR_OP_MATCHES { $id = $STR_OP_MATCHES.text; }
;
