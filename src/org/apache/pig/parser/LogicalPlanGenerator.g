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
import org.apache.pig.impl.builtin.GFAny;
import org.apache.pig.impl.builtin.ReadScalars;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
}

@members {
private static Log log = LogFactory.getLog( LogicalPlanGenerator.class );

private LogicalPlanBuilder builder = new LogicalPlanBuilder();

private boolean inForeachPlan = false;
private LogicalRelationalOperator currentOp = null; // Current relational operator that's being built.

public LogicalPlan getLogicalPlan() {
    return builder.getPlan();
}

public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    String msg = e.getMessage();
    if ( e instanceof NonProjectExpressionException ) {
        NonProjectExpressionException pee = (NonProjectExpressionException)e;
        msg = "For input to a nested operator, if it's an expression, it can only be " +
              "a projection expression. The given expression is: " + 
              pee.getExpression().getPlan() + ".";
    } else {
        msg = e.toString();
    }
    
    return msg;
}

} // End of @members

query : ^( QUERY statement* )
;

statement
scope {
   // Parsing context
    String alias; // The alias of the current operator, either given or generated by the parser.
    Integer parallel; // Parallelism
    String inputAlias; // The alias of the input operator
    int inputIndex;
}
@init {
    $statement::inputIndex = 0;
}
 : general_statement
 | foreach_statement
 | split_statement
;

split_statement : split_clause
;

general_statement 
: ^( STATEMENT ( alias { $statement::alias = $alias.name; } )? op_clause parallel_clause? )
;

parallel_clause
 : ^( PARALLEL INTEGER )
   {
       $statement::parallel = Integer.parseInt( $INTEGER.text );
   }
;

// We need to handle foreach specifically because of the ending ';', which is not required 
// if there is a nested block. This is ugly, but it gets the job done.
foreach_statement : ^( STATEMENT ( alias { $statement::alias = $alias.name; } )? foreach_clause )
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
          | stream_clause { $alias = $stream_clause.alias; }
          | mr_clause { $alias = $mr_clause.alias; }
;

define_clause 
 : ^( DEFINE alias cmd[$alias.name] ) 
   {
       builder.defineCommand( $alias.name, $cmd.command );
   }
 | ^( DEFINE alias func_clause )
   {
       builder.defineFunction( $alias.name, $func_clause.funcSpec );
   }
;

cmd[String alias] returns[StreamingCommand command]
@init {
    List<String> shipPaths = new ArrayList<String>();
    List<String> cachePaths = new ArrayList<String>();
}
 : ^( EXECCOMMAND ( ship_clause[shipPaths] | cache_caluse[cachePaths] | input_clause | output_clause | error_clause )* )
   {
       $command = builder.buildCommand( $EXECCOMMAND.text, shipPaths,
           cachePaths, $input_clause.inputHandleSpecs, $output_clause.outputHandleSpecs,
           $error_clause.dir == null? $alias : $error_clause.dir, $error_clause.limit, input );
   }
;

ship_clause[List<String> paths]
 : ^( SHIP path_list[$paths]? )
;

path_list[List<String> paths]
 : ( QUOTEDSTRING { $paths.add( builder.unquote( $QUOTEDSTRING.text ) ); } )+
;

cache_caluse[List<String> paths]
 : ^( CACHE path_list[$paths] )
;

input_clause returns[List<HandleSpec> inputHandleSpecs]
@init {
    $inputHandleSpecs = new ArrayList<HandleSpec>();
}
 : ^( INPUT ( stream_cmd { $inputHandleSpecs.add( $stream_cmd.handleSpec ); } )+ )
;

stream_cmd returns[HandleSpec handleSpec]
@init {
    String handleName = null;
    FuncSpec fs = null;
    String deserializer = PigStreaming.class.getName() + "()";
    if( fs != null )
        deserializer =  fs.toString();
}
@final {
    $handleSpec = new HandleSpec( handleName, deserializer );
}
 : ^( STDIN { handleName = "stdin"; }
      ( func_clause { fs = $func_clause.funcSpec; } )? )
 | ^( STDOUT { handleName = "stdout"; }
      ( func_clause { fs = $func_clause.funcSpec; } )? )
 | ^( QUOTEDSTRING { handleName = builder.unquote( $QUOTEDSTRING.text ); }
      ( func_clause { fs = $func_clause.funcSpec; } )? )
;

output_clause returns[List<HandleSpec> outputHandleSpecs]
@init {
    $outputHandleSpecs = new ArrayList<HandleSpec>();
}
 : ^( OUTPUT ( stream_cmd { $outputHandleSpecs.add( $stream_cmd.handleSpec ); } )+ )
;

error_clause returns[String dir, Integer limit]
@init {
    $limit = StreamingCommand.MAX_TASKS;
}
 : ^( STDERROR QUOTEDSTRING INTEGER? )
   {
       $dir = builder.unquote( $QUOTEDSTRING.text );
       $limit = Integer.parseInt( $INTEGER.text );
   }
;

load_clause returns[String alias]
 : ^( LOAD filename func_clause? as_clause? )
  {
      $alias = builder.buildLoadOp( $statement::alias,
          $statement::parallel, $filename.filename, $func_clause.funcSpec, $as_clause.logicalSchema  );
  }
;

filename returns[String filename]
 : QUOTEDSTRING { $filename = builder.unquote( $QUOTEDSTRING.text ); }
;

as_clause returns[LogicalSchema logicalSchema]
 : ^( AS field_def_list ) { $logicalSchema = $field_def_list.schema; }
;

field_def returns[LogicalFieldSchema fieldSchema]
 : ^( FIELD_DEF IDENTIFIER type )
   {
       $fieldSchema = new LogicalFieldSchema( $IDENTIFIER.text, $type.logicalSchema, $type.datatype );
   }
;

field_def_list returns[LogicalSchema schema]
@init {
    $schema = new LogicalSchema();
}
 : ( field_def { $schema.addField( $field_def.fieldSchema ); } )+
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
 : ^( TUPLE_TYPE field_def_list )
   { 
       $logicalSchema = $field_def_list.schema;
   }
;

bag_type returns[LogicalSchema logicalSchema]
 : ^( BAG_TYPE tuple_type? )
   { 
       $logicalSchema = $tuple_type.logicalSchema;
   }
;

map_type : MAP_TYPE
;

func_clause returns[FuncSpec funcSpec]
 : ^( FUNC func_name func_args? )
   { 
       $funcSpec = builder.buildFuncSpec( $func_name.funcName, $func_args.args );
   }
 | ^( FUNC_REF func_alias )
   {
       $funcSpec = builder.lookupFunction( $func_alias.alias );
       if( $funcSpec == null )
           $funcSpec = builder.buildFuncSpec( $func_alias.alias, new ArrayList<String>() );
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
scope {
    MultiMap<Integer, LogicalExpressionPlan> groupPlans;
    int inputIndex;
    List<String> inputAliases;
    List<Boolean> innerFlags;
}
@init {
    currentOp = builder.createGroupOp(); 
    $group_clause::groupPlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $group_clause::inputAliases = new ArrayList<String>();
    $group_clause::innerFlags = new ArrayList<Boolean>();
    GROUPTYPE groupType = GROUPTYPE.REGULAR;
}
 : ^( GROUP group_item+ ( group_type { groupType = $group_type.type; } )? )
   {
       $alias = builder.buildGroupOp( (LOCogroup)currentOp, $statement::alias, $statement::parallel, 
           $group_clause::inputAliases, $group_clause::groupPlans, groupType, $group_clause::innerFlags );
   }
 | ^( COGROUP group_item+ ( group_type { groupType = $group_type.type; } )? )
   {
       $alias = builder.buildGroupOp( (LOCogroup)currentOp, $statement::alias, $statement::parallel, 
           $group_clause::inputAliases, $group_clause::groupPlans, groupType, $group_clause::innerFlags );
   }
;

group_type returns[GROUPTYPE type]
 : HINT_COLLECTED { $type = GROUPTYPE.COLLECTED; }
 | HINT_MERGE { $type = GROUPTYPE.MERGE; }
 | HINT_REGULAR { $type = GROUPTYPE.REGULAR; }
;

group_item
@init { boolean inner = false; }
 : rel ( join_group_by_clause 
         { 
             $group_clause::groupPlans.put( $group_clause::inputIndex, $join_group_by_clause.plans );
         }
         | ALL 
         {
             LogicalExpressionPlan plan = new LogicalExpressionPlan();
             new ConstantExpression( plan, "all", new LogicalFieldSchema( null , null, DataType.CHARARRAY ) );
             List<LogicalExpressionPlan> plans = new ArrayList<LogicalExpressionPlan>( 1 );
             plans.add( plan );
             $group_clause::groupPlans.put( $group_clause::inputIndex, plans );
         }
         | ANY
         {
             LogicalExpressionPlan plan = new LogicalExpressionPlan();
             new UserFuncExpression( plan, new FuncSpec( GFAny.class.getName() ) );
             List<LogicalExpressionPlan> plans = new ArrayList<LogicalExpressionPlan>( 1 );
             plans.add( plan );
             $group_clause::groupPlans.put( $group_clause::inputIndex, plans );
         }
        ) ( INNER { inner =  true; } | OUTER )?
   {
       $group_clause::inputAliases.add( $statement::inputAlias );
       $group_clause::innerFlags.add( inner );
       $group_clause::inputIndex++;
       $statement::inputIndex++;
   }
;

rel
 : alias
   {
       $statement::inputAlias = $alias.name;
   }
 | op_clause
   {
       $statement::inputAlias = $op_clause.alias;
   }
;

flatten_generated_item returns[LogicalExpressionPlan plan, boolean flattenFlag, LogicalSchema schema]
@init {
    $plan = new LogicalExpressionPlan();
}
 : ( flatten_clause[$plan] { $flattenFlag = true; }
   | expr[$plan]
   | STAR
     {
         builder.buildProjectExpr( $plan, currentOp, $statement::inputIndex, null, -1 );
     }
   )
   ( field_def_list { $schema = $field_def_list.schema; } )?
;

flatten_clause[LogicalExpressionPlan plan]
 : ^( FLATTEN expr[$plan] )
;

store_clause returns[String alias]
 : ^( STORE alias filename func_clause? )
   {
       $alias= builder.buildStoreOp( $statement::alias,
          $statement::parallel, $alias.name, $filename.filename, $func_clause.funcSpec );
   }
;

filter_clause returns[String alias]
@init { 
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
    currentOp = builder.createFilterOp();
}
 : ^( FILTER rel cond[exprPlan] )
   {
       $alias = builder.buildFilterOp( (LOFilter)currentOp, $statement::alias,
          $statement::parallel, $statement::inputAlias, exprPlan );
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
   {
       $expr = $func_eval.expr;
   }
;

func_eval[LogicalExpressionPlan plan] returns[LogicalExpression expr]
@init { 
    List<LogicalExpression> args = new ArrayList<LogicalExpression>();
}
 : ^( FUNC_EVAL func_name ( real_arg[$plan] { args.add( $real_arg.expr ); } )* )
   {
       $expr = builder.buildUDF( $plan, $func_name.funcName, args );
   }
;

real_arg [LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : e = expr[$plan] { $expr = $e.expr; }
 | STAR
   {
       $expr = builder.buildProjectExpr( $plan, currentOp, $statement::inputIndex, null, -1 );
   }
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
       $expr = $const_expr.expr;
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
@init {
    List<Object> columns = new ArrayList<Object>();
}
 : projectable_expr[$plan] { $expr = $projectable_expr.expr; }
   ( dot_proj 
     {
         if( $expr instanceof ScalarExpression ) {
             // This is a scalar projection.
             ScalarExpression scalarExpr = (ScalarExpression)$expr;
             if( $dot_proj.cols.size() > 1 ) {
                 throw new InvalidScalarProjectionException( input, scalarExpr );
             } else {
                 Object val = $dot_proj.cols.get( 0 );
                 int pos = -1;
                 LogicalRelationalOperator relOp = (LogicalRelationalOperator)scalarExpr.getImplicitReferencedOperator();
                 LogicalSchema schema = null;
                 try {
                     schema = relOp.getSchema();
                 } catch(FrontendException e) {
                     throw new PlanGenerationFailureException( input, e );
                 }
                 if( val instanceof Integer ) {
                     pos = (Integer)val;
                     if( schema != null && pos >= schema.size() ) {
                         throw new InvalidScalarProjectionException( input, scalarExpr );
                     }
                 } else {
                     String colAlias = (String)val;
                     pos = schema.getFieldPosition( colAlias );
                     if( schema == null || pos == -1 ) {
                         throw new InvalidScalarProjectionException( input, scalarExpr );
                     }
                 }
                 LogicalFieldSchema fs = new LogicalFieldSchema( null , null, DataType.INTEGER );
                 ConstantExpression constExpr = new ConstantExpression( $plan, pos, fs  );
                 plan.connect( $expr, constExpr );
                 fs = new LogicalFieldSchema( null , null, DataType.CHARARRAY );
                 constExpr = new ConstantExpression( $plan, "filename", fs ); // place holder for file name.
                 plan.connect( $expr, constExpr );
             }
         } else {
             DereferenceExpression e = new DereferenceExpression( $plan );
             e.setRawColumns( $dot_proj.cols );
             $plan.connect( $expr, e );
             $expr = e;
         }
     }
   | pound_proj
     {
         MapLookupExpression e = new MapLookupExpression( $plan, $pound_proj.key, null );
         $plan.connect( $expr, e );
         $expr = e;
     }
  )*
;

projectable_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : func_eval[$plan]
   {
       $expr = $func_eval.expr;
   }
 | col_ref[$plan]
   {
       $expr = $col_ref.expr;
   }
 | bin_expr[$plan]
   {
       $expr = $bin_expr.expr;
   }
;

dot_proj returns[List<Object> cols]
@init {
    $cols = new ArrayList<Object>();
}
 : ^( PERIOD ( col_alias_or_index { $cols.add( $col_alias_or_index.col ); } )+ )
;

col_alias_or_index returns[Object col]
 : col_alias { $col = $col_alias.col; } | col_index { $col = $col_index.col; }
;

col_alias returns[Object col]
 : GROUP { $col = $GROUP.text; } | IDENTIFIER { $col = $IDENTIFIER.text; }
;

col_index returns[Object col]
 : ^( DOLLAR INTEGER { $col = Integer.valueOf( $INTEGER.text ); } )
;

pound_proj returns[String key]
 : ^( POUND ( QUOTEDSTRING { $key = builder.unquote( $QUOTEDSTRING.text ); } | NULL ) )
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
       $alias = builder.buildLimitOp( $statement::alias,
           $statement::parallel, $statement::inputAlias, Long.valueOf( $INTEGER.text ) );
   }
 | ^( LIMIT rel LONGINTEGER )
   {
       $alias = builder.buildLimitOp( $statement::alias,
           $statement::parallel, $statement::inputAlias, Long.valueOf( $LONGINTEGER.text ) );
   }
;

sample_clause returns[String alias]
 : ^( SAMPLE rel DOUBLENUMBER )
   {
       $alias = builder.buildSampleOp( $statement::alias,
           $statement::parallel, $statement::inputAlias, Double.valueOf( $DOUBLENUMBER.text ) );
   }
;

order_clause returns[String alias]
@init {
    currentOp = builder.createSortOp();
}
 : ^( ORDER rel order_by_clause func_clause? )
   {
       $alias = builder.buildSortOp( (LOSort)currentOp, $statement::alias,
           $statement::parallel, $statement::inputAlias, $order_by_clause.plans, 
           $order_by_clause.ascFlags, $func_clause.funcSpec );
   }
;

order_by_clause returns[List<LogicalExpressionPlan> plans, List<Boolean> ascFlags]
@init {
    $plans = new ArrayList<LogicalExpressionPlan>();
    $ascFlags = new ArrayList<Boolean>();
}
 : STAR {
       LogicalExpressionPlan plan = new LogicalExpressionPlan();
       builder.buildProjectExpr( plan, currentOp, $statement::inputIndex, null, -1 );
       $plans.add( plan );
   }
   ( ASC { $ascFlags.add( true ); } | DESC { $ascFlags.add( false ); } )?
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
       $alias = builder.buildDistinctOp( $statement::alias,
          $statement::parallel, $statement::inputAlias, $partition_clause.partitioner );
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
       $alias = builder.buildCrossOp( $statement::alias,
          $statement::parallel, $rel_list.aliasList, $partition_clause.partitioner );
   }
;

rel_list returns[List<String> aliasList]
@init { $aliasList = new ArrayList<String>(); }
 : ( rel { $aliasList.add( $statement::inputAlias ); } )+
;

join_clause returns[String alias]
scope {
    MultiMap<Integer, LogicalExpressionPlan> joinPlans;
    int inputIndex;
    List<String> inputAliases;
    List<Boolean> innerFlags;
}
@init {
    currentOp = builder.createJoinOp();
    $join_clause::joinPlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $join_clause::inputAliases = new ArrayList<String>();
    $join_clause::innerFlags = new ArrayList<Boolean>();
}
 : ^( JOIN join_sub_clause join_type? partition_clause? )
   {
       $alias = builder.buildJoinOp( (LOJoin)currentOp, $statement::alias,
          $statement::parallel, $join_clause::inputAliases, $join_clause::joinPlans,
          $join_type.type, $join_clause::innerFlags, $partition_clause.partitioner );
   }
;

join_type returns[JOINTYPE type]
 : HINT_REPL { $type = JOINTYPE.REPLICATED; }
 | HINT_MERGE { $type = JOINTYPE.MERGE; }
 | HINT_SKEWED { $type = JOINTYPE.SKEWED; }
 | HINT_DEFAULT { $type = JOINTYPE.HASH; }
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
 : ^( JOIN_ITEM rel join_group_by_clause )
   {
       $join_clause::inputAliases.add( $statement::inputAlias );
       $join_clause::joinPlans.put( $join_clause::inputIndex, $join_group_by_clause.plans );
       $join_clause::inputIndex++;
       $statement::inputIndex++;
   }
;

join_group_by_clause returns[List<LogicalExpressionPlan> plans]
@init {
    $plans = new ArrayList<LogicalExpressionPlan>();
}
 : ^( BY ( join_group_by_expr { $plans.add( $join_group_by_expr.plan ); } )+ )
;

join_group_by_expr returns[LogicalExpressionPlan plan]
@init {
    $plan = new LogicalExpressionPlan();
}
 : expr[$plan]
 | STAR 
   {
       builder.buildProjectExpr( $plan, currentOp, $statement::inputIndex, null, -1 );
   }
;

union_clause returns[String alias]
@init {
    boolean onSchema = false;
}
 : ^( UNION ( ONSCHEMA { onSchema = true; } )? rel_list )
   {
      $alias = builder.buildUnionOp( $statement::alias,
          $statement::parallel, $rel_list.aliasList, onSchema );
   }
;

foreach_clause returns[String alias]
scope {
    LOForEach foreachOp;
}
@init {
     $foreach_clause::foreachOp = builder.createForeachOp();
}
 : ^( FOREACH rel foreach_plan )
   {
       $alias = builder.buildForeachOp( $foreach_clause::foreachOp, $statement::alias,
          $statement::parallel, $statement::inputAlias, $foreach_plan.plan );
   }
;

foreach_plan returns[LogicalPlan plan]
scope {
    LogicalPlan innerPlan;
    Map<String, LogicalExpressionPlan> exprPlans;
    Map<String, Operator> operators;
}
@init {
    inForeachPlan = true;
    $foreach_plan::innerPlan = new LogicalPlan();
    $foreach_plan::exprPlans = new HashMap<String, LogicalExpressionPlan>();
    $foreach_plan::operators = new HashMap<String, Operator>();
}
@after {
    $plan = $foreach_plan::innerPlan;
    inForeachPlan = false;
}
 : ^( FOREACH_PLAN nested_blk )
 | ^( FOREACH_PLAN_SIMPLE generate_clause parallel_clause? )
;

nested_blk : nested_command* generate_clause
;

generate_clause
@init {
    currentOp = builder.createGenerateOp( $foreach_plan::innerPlan );
    List<LogicalExpressionPlan> plans = new ArrayList<LogicalExpressionPlan>();
    List<Boolean> flattenFlags = new ArrayList<Boolean>();
    List<LogicalSchema> schemas = new ArrayList<LogicalSchema>();
}
 : ^( GENERATE ( flatten_generated_item
                 {
                     plans.add( $flatten_generated_item.plan );
                     flattenFlags.add( $flatten_generated_item.flattenFlag );
                     schemas.add( $flatten_generated_item.schema );
                 }
               )+
    )
   {   
       builder.buildGenerateOp( $foreach_clause::foreachOp, (LOGenerate)currentOp,
           $foreach_plan::operators,
           plans, flattenFlags, schemas );
   }
;

nested_command
@init {
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
}
 : ^( NESTED_CMD IDENTIFIER nested_op[$IDENTIFIER.text] )
   {
       $foreach_plan::operators.put( $IDENTIFIER.text, $nested_op.op );
   }
 | 
   ^( NESTED_CMD_ASSI IDENTIFIER expr[exprPlan] )
   {
        $foreach_plan::exprPlans.put( $IDENTIFIER.text, exprPlan );
   }
;

nested_op[String alias] returns[Operator op]
 : nested_proj[$alias] { $op = $nested_proj.op; }
 | nested_filter[$alias] { $op = $nested_filter.op; }
 | nested_sort [$alias] { $op = $nested_sort.op; }
 | nested_distinct[$alias] { $op = $nested_distinct.op; }
 | nested_limit[$alias] { $op = $nested_limit.op; }
;

nested_proj[String alias] returns[Operator op]
@init {
    LogicalExpressionPlan plan = new LogicalExpressionPlan();
    List<LogicalExpressionPlan> plans = new ArrayList<LogicalExpressionPlan>();
}
 : ^( NESTED_PROJ
      cr0 = col_ref[plan]
      ( cr = col_ref[new LogicalExpressionPlan()]
        {
            plans.add( (LogicalExpressionPlan)( $cr.expr.getPlan() ) );
        }
      )+ )
   {
       $op = builder.buildNestedProjectOp( $foreach_plan::innerPlan, $foreach_clause::foreachOp, 
           $foreach_plan::operators, $alias, (ProjectExpression)$cr0.expr, plans );
   }
;

nested_filter[String alias] returns[Operator op]
@init {
    LogicalExpressionPlan plan = new LogicalExpressionPlan();
    Operator inputOp = null;
    currentOp = builder.createNestedFilterOp( $foreach_plan::innerPlan );
}
 : ^( FILTER nested_op_input cond[plan] )
   {
       $op = builder.buildNestedFilterOp( (LOFilter)currentOp, $foreach_plan::innerPlan, $alias, 
           $nested_op_input.op, plan );
   }
;

nested_sort[String alias] returns[Operator op]
@init {
    Operator inputOp = null;
    currentOp = builder.createNestedSortOp( $foreach_plan::innerPlan );
}
 : ^( ORDER nested_op_input order_by_clause func_clause? )
   {
       $op = builder.buildNestedSortOp( (LOSort)currentOp, $foreach_plan::innerPlan, $alias,
           $nested_op_input.op, 
           $order_by_clause.plans, $order_by_clause.ascFlags, $func_clause.funcSpec );
   }
;

nested_distinct[String alias] returns[Operator op]
@init {
    Operator inputOp = null;
}
 : ^( DISTINCT nested_op_input )
   {
       $op = builder.buildNestedDistinctOp( $foreach_plan::innerPlan, $alias, $nested_op_input.op );
   }
;

nested_limit[String alias] returns[Operator op]
@init {
    Operator inputOp = null;
}
 : ^( LIMIT nested_op_input INTEGER )
   {
       $op = builder.buildNestedLimitOp( $foreach_plan::innerPlan, $alias, $nested_op_input.op, 
           Integer.valueOf( $INTEGER.text ) );
   }
;

nested_op_input returns[Operator op]
@init {
    LogicalExpressionPlan plan = new LogicalExpressionPlan();
}
 : col_ref[plan]
   {
       $op = builder.buildNestedOperatorInput( $foreach_plan::innerPlan,
           $foreach_clause::foreachOp, $foreach_plan::operators, $col_ref.expr, input );
   }
 | nested_proj[null]
   { 
       $op = $nested_proj.op;
   }
;

stream_clause returns[String alias]
@init {
    StreamingCommand cmd = null;
}
 : ^( STREAM rel ( EXECCOMMAND { cmd = builder.buildCommand( $EXECCOMMAND.text, input ); } 
                 | IDENTIFIER { cmd = builder.lookupCommand( $IDENTIFIER.text ); } ) as_clause? )
   {
       $alias = builder.buildStreamOp( $statement::alias, $statement::parallel,
          $statement::inputAlias, cmd, $as_clause.logicalSchema, input );
   }
;

mr_clause returns[String alias]
@init {
    List<String> paths = new ArrayList<String>();
    String alias = $statement::alias;
}
 : ^( MAPREDUCE QUOTEDSTRING path_list[paths]? 
     { $statement::alias = null; } store_clause 
     { $statement::alias = alias; } load_clause
     EXECCOMMAND? )
   {
       $alias = builder.buildNativeOp( $statement::parallel,
           builder.unquote( $QUOTEDSTRING.text ), builder.unquote( $EXECCOMMAND.text ), 
           paths, $store_clause.alias, $load_clause.alias, input );
   }
;

split_clause
 : ^( SPLIT rel { $statement::inputAlias = builder.buildSplitOp( $statement::inputAlias ); } 
      split_branch+ )
;

split_branch
@init {
    LogicalExpressionPlan splitPlan = new LogicalExpressionPlan();
    currentOp = builder.createSplitOutputOp();
}
 : ^( SPLIT_BRANCH IDENTIFIER cond[splitPlan] )
   {
       builder.buildSplitOutputOp( (LOSplitOutput)currentOp, $IDENTIFIER.text,
           $statement::parallel, $statement::inputAlias, splitPlan );
   }
;

col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : alias_col_ref[$plan] { $expr = $alias_col_ref.expr; }
 | dollar_col_ref[$plan] { $expr = $dollar_col_ref.expr; }
;

alias_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : GROUP 
   {
       $expr = builder.buildProjectExpr( $plan, currentOp, 
           $statement::inputIndex, $GROUP.text, 0 );
   }
 | IDENTIFIER
   {
       String alias = $IDENTIFIER.text;
       Operator inOp = builder.lookupOperator( $statement::inputAlias );
       LogicalSchema schema;
       try {
           schema = ((LogicalRelationalOperator)inOp).getSchema();
       } catch (FrontendException e) {
           throw new PlanGenerationFailureException( input, e );
       }
       
       Operator op = builder.lookupOperator( alias );
       if( op != null && ( schema == null || schema.getFieldPosition( alias ) == -1 ) ) {
           $expr = new ScalarExpression( plan, op,
               inForeachPlan ? $foreach_clause::foreachOp : currentOp );
       } else {
           if( inForeachPlan ) {
               $expr = builder.buildProjectExpr( $plan, currentOp, 
                   $foreach_plan::exprPlans, alias, 0 );
           } else {
               $expr = builder.buildProjectExpr( $plan, currentOp, 
                   $statement::inputIndex, alias, 0 );
           }
       }
   }
;

dollar_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( DOLLAR INTEGER )
   {
       int col = Integer.valueOf( $INTEGER.text );
       $expr = builder.buildProjectExpr( $plan, currentOp, 
           $statement::inputIndex, null, col );
   }
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
    | STDERROR { $id = $STDERROR.text; }
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
