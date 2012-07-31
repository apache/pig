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

scope GScope {
    LogicalRelationalOperator currentOp; // Current relational operator that's being built.
}

@header {
package org.apache.pig.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.GFAny;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.NumValCarrier;
import org.apache.pig.impl.plan.PlanValidationException;
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
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOLimit;
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
import java.util.Arrays;
}

@members {
private static Log log = LogFactory.getLog( LogicalPlanGenerator.class );

private LogicalPlanBuilder builder = null;

private boolean inForeachPlan = false;

private boolean inNestedCommand = false;

public LogicalPlan getLogicalPlan() {
    return builder.getPlan();
}

public Map<String, Operator> getOperators() {
    return builder.getOperators();
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

public LogicalPlanGenerator(TreeNodeStream input, LogicalPlanBuilder builder) {
    this(input, new RecognizerSharedState());
    this.builder = builder;
}

public LogicalPlanGenerator(TreeNodeStream input, PigContext pigContext, String scope,
    Map<String, String> fileNameMap) {
    this( input );
    builder = new LogicalPlanBuilder( pigContext, scope, fileNameMap, input );
}

} // End of @members

@rulecatch {
catch(RecognitionException re) {
    throw re;
}
}

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
 | split_statement
 | realias_statement
;

split_statement : split_clause
;

realias_statement : realias_clause
;

general_statement 
: ^( STATEMENT ( alias { $statement::alias = $alias.name; } )? oa = op_clause parallel_clause? )
  {
      Operator op = builder.lookupOperator( $oa.alias );
      builder.setParallel( (LogicalRelationalOperator)op, $statement::parallel );
  }
;

realias_clause
: ^(REALIAS alias IDENTIFIER)
    {
	    Operator op = builder.lookupOperator( $IDENTIFIER.text );
	    if (op==null) {
	        throw new UndefinedAliasException(input, 
	            new SourceLocation( (PigParserNode)$IDENTIFIER ), $IDENTIFIER.text);
	    }
	    builder.putOperator( $alias.name, (LogicalRelationalOperator)op );
    }
;

parallel_clause
 : ^( PARALLEL INTEGER )
   {
       $statement::parallel = Integer.parseInt( $INTEGER.text );
   }
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
          | foreach_clause { $alias = $foreach_clause.alias; }
          | cube_clause { $alias = $cube_clause.alias; }
;

define_clause 
 : ^( DEFINE alias cmd[$alias.name] ) 
   {
       builder.defineCommand( $alias.name, $cmd.command );
   }
 | ^( DEFINE alias func_clause[FunctionType.UNKNOWNFUNC] )
   {
       builder.defineFunction( $alias.name, $func_clause.funcSpec );
   }
;

cmd[String alias] returns[StreamingCommand command]
@init {
    List<String> shipPaths = new ArrayList<String>();
    List<String> cachePaths = new ArrayList<String>();
    SourceLocation loc = new SourceLocation( (PigParserNode)$cmd.start );
}
 : ^( EXECCOMMAND ( ship_clause[shipPaths] | cache_clause[cachePaths] | input_clause | output_clause | error_clause )* )
   {
       $command = builder.buildCommand( loc, builder.unquote( $EXECCOMMAND.text ), shipPaths,
           cachePaths, $input_clause.inputHandleSpecs, $output_clause.outputHandleSpecs,
           $error_clause.dir, $error_clause.limit );
   }
;

ship_clause[List<String> paths]
 : ^( SHIP path_list[$paths]? )
;

path_list[List<String> paths]
 : ( QUOTEDSTRING { $paths.add( builder.unquote( $QUOTEDSTRING.text ) ); } )+
;

cache_clause[List<String> paths]
 : ^( CACHE path_list[$paths] )
;

input_clause returns[List<HandleSpec> inputHandleSpecs]
@init {
    $inputHandleSpecs = new ArrayList<HandleSpec>();
}
 : ^( INPUT ( stream_cmd[true] { $inputHandleSpecs.add( $stream_cmd.handleSpec ); } )+ )
;

stream_cmd[boolean in] returns[HandleSpec handleSpec]
@init {
    String handleName = null;
    FuncSpec fs = null;
    String deserializer = PigStreaming.class.getName() + "()";
    byte ft = $in ? FunctionType.PIGTOSTREAMFUNC : FunctionType.STREAMTOPIGFUNC;
}
@after {
    if( fs != null )
        deserializer =  fs.toString();
    $handleSpec = new HandleSpec( handleName, deserializer );
}
 : ^( STDIN { handleName = "stdin"; }
      ( func_clause[ft] { fs = $func_clause.funcSpec; } )? )
 | ^( STDOUT { handleName = "stdout"; }
      ( func_clause[ft] { fs = $func_clause.funcSpec; } )? )
 | ^( QUOTEDSTRING { handleName = builder.unquote( $QUOTEDSTRING.text ); }
      ( func_clause[ft] { fs = $func_clause.funcSpec; } )? )
;

output_clause returns[List<HandleSpec> outputHandleSpecs]
@init {
    $outputHandleSpecs = new ArrayList<HandleSpec>();
}
 : ^( OUTPUT ( stream_cmd[false] { $outputHandleSpecs.add( $stream_cmd.handleSpec ); } )+ )
;

error_clause returns[String dir, Integer limit]
@init {
    $limit = StreamingCommand.MAX_TASKS;
}
 : ^( STDERROR 
      ( QUOTEDSTRING 
        {
            $dir = builder.unquote( $QUOTEDSTRING.text );
        }
        ( INTEGER 
          { 
              $limit = Integer.parseInt( $INTEGER.text );
          }
        )?
      )?
    )
;

load_clause returns[String alias]
 : ^( LOAD filename func_clause[FunctionType.LOADFUNC]? as_clause? )
  {
      SourceLocation loc = new SourceLocation( (PigParserNode)$load_clause.start );
      $alias = builder.buildLoadOp( loc, $statement::alias,
          $filename.filename, $func_clause.funcSpec, $as_clause.logicalSchema  );
  }
;

filename returns[String filename]
 : QUOTEDSTRING { $filename = builder.unquote( $QUOTEDSTRING.text ); }
;

as_clause returns[LogicalSchema logicalSchema]
 : ^( AS field_def_list ) 
   { 
        LogicalPlanBuilder.setBytearrayForNULLType($field_def_list.schema);
        $logicalSchema = $field_def_list.schema; 
   }
;

field_def[NumValCarrier nvc] returns[LogicalFieldSchema fieldSchema]
@init {
    byte datatype = DataType.NULL;          
    if ($nvc==null) {
        $nvc=new NumValCarrier();
    }
}
 : ^( FIELD_DEF IDENTIFIER ( type { datatype = $type.datatype;} )? )
   {
           $fieldSchema = new LogicalFieldSchema( $IDENTIFIER.text, $type.logicalSchema, datatype );
   }
 | ^( FIELD_DEF_WITHOUT_IDENTIFIER ( type { datatype = $type.datatype; } ) )
   {
           $fieldSchema = new LogicalFieldSchema ( $nvc.makeNameFromDataType(datatype) , $type.logicalSchema, datatype );
   }
;

field_def_list returns[LogicalSchema schema]
@init {
    $schema = new LogicalSchema();
    NumValCarrier nvc = new NumValCarrier();
}
 : ( field_def[nvc] { $schema.addField( $field_def.fieldSchema ); } )+
;


type returns[Byte datatype, LogicalSchema logicalSchema]
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
       $logicalSchema = $map_type.logicalSchema;
   }
;

simple_type returns[byte datatype]
 : BOOLEAN { $datatype = DataType.BOOLEAN; }
 | INT { $datatype = DataType.INTEGER; }
 | LONG { $datatype = DataType.LONG; }
 | FLOAT { $datatype = DataType.FLOAT; }
 | DOUBLE { $datatype = DataType.DOUBLE; }
 | CHARARRAY { $datatype = DataType.CHARARRAY; }
 | BYTEARRAY { $datatype = DataType.BYTEARRAY; }
;

tuple_type returns[LogicalSchema logicalSchema]
 : ^( TUPLE_TYPE 
      ( field_def_list
        { 
            LogicalPlanBuilder.setBytearrayForNULLType($field_def_list.schema);
            $logicalSchema = $field_def_list.schema;
        }
      )?
    )
;

bag_type returns[LogicalSchema logicalSchema]
 : ^( BAG_TYPE IDENTIFIER? tuple_type? )
   {
       if ($tuple_type.logicalSchema!=null && $tuple_type.logicalSchema.size()==1 && $tuple_type.logicalSchema.getField(0).type==DataType.TUPLE) {
           $logicalSchema = $tuple_type.logicalSchema;
       }
       else {
           LogicalSchema s = new LogicalSchema();
           s.addField(new LogicalFieldSchema($IDENTIFIER.text, $tuple_type.logicalSchema, DataType.TUPLE));
           $logicalSchema = s;
       }
   }
;

map_type returns[LogicalSchema logicalSchema]
 : ^( MAP_TYPE type? )
   {
       LogicalSchema s = null;
       if( $type.datatype != null ) {
           s = new LogicalSchema();
           s.addField( new LogicalFieldSchema( null, $type.logicalSchema, $type.datatype ) );
       }
       $logicalSchema = s;
   }
;

func_clause[byte ft] returns[FuncSpec funcSpec]
@init {
    SourceLocation loc = new SourceLocation( (PigParserNode)$func_clause.start );
}
 : ^( FUNC_REF func_name )
   {
       $funcSpec = builder.lookupFunction( $func_name.funcName );
       if( $funcSpec == null )
           $funcSpec = builder.buildFuncSpec( loc, $func_name.funcName, new ArrayList<String>(), $ft );
   }
 | ^( FUNC func_name func_args? )
   {
       $funcSpec = builder.lookupFunction( $func_name.funcName );
       if( $funcSpec == null ) {
           List<String> argList = new ArrayList<String>();
           if( $func_args.args != null )
               argList = $func_args.args;
           $funcSpec = builder.buildFuncSpec( loc, $func_name.funcName, argList, $ft );
       }
   }
;

func_name returns[String funcName]
@init { StringBuilder buf = new StringBuilder(); } 
 : p1 = eid { buf.append( $p1.id ); }
      ( ( PERIOD { buf.append( $PERIOD.text ); } | DOLLAR { buf.append( $DOLLAR.text ); } )
      p2 = eid { buf.append( $p2.id ); } )*
   {
       $funcName = buf.toString();
   }
;

func_args returns[List<String> args]
@init { $args = new ArrayList<String>(); }
: ( QUOTEDSTRING { $args.add( builder.unquote( $QUOTEDSTRING.text ) ); } 
    | MULTILINE_QUOTEDSTRING { $args.add( builder.unquote( $MULTILINE_QUOTEDSTRING.text ) ); }
  )+
;

// Sets the current operator as CUBE and creates LogicalExpressionPlans based on the user input.
// Ex: x = CUBE inp BY CUBE(a,b), ROLLUP(c,d);
// For the above example this grammar creates LogicalExpressionPlan with ProjectExpression for a,b and c,d dimensions.
// It also outputs the order of operations i.e in this case CUBE operation followed by ROLLUP operation
// These inputs are passed to buildCubeOp methods which then builds the logical plan for CUBE operator.
// If user specifies STAR or RANGE expression for dimensions then it will be expanded inside buildCubeOp.
cube_clause returns[String alias]
scope {
    LOCube cubeOp;
    MultiMap<Integer, LogicalExpressionPlan> cubePlans;
    List<String> operations;
    int inputIndex;
}
scope GScope;
@init {
    $cube_clause::cubeOp = builder.createCubeOp();
    $GScope::currentOp = $cube_clause::cubeOp;
    $cube_clause::cubePlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $cube_clause::operations = new ArrayList<String>();
}
 : ^( CUBE cube_item )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$cube_clause.start );
       $alias = builder.buildCubeOp( loc, $cube_clause::cubeOp, $statement::alias, 
       $statement::inputAlias, $cube_clause::operations, $cube_clause::cubePlans );
   }
;

cube_item
 : rel ( cube_by_clause 
   { 
       $cube_clause::cubePlans = $cube_by_clause.plans;
       $cube_clause::operations = $cube_by_clause.operations;
   } )
;

cube_by_clause returns[List<String> operations, MultiMap<Integer, LogicalExpressionPlan> plans]
@init {
    $operations = new ArrayList<String>();
    $plans = new MultiMap<Integer, LogicalExpressionPlan>();
}
 : ^( BY cube_or_rollup { $operations = $cube_or_rollup.operations; $plans = $cube_or_rollup.plans; })
;

cube_or_rollup returns[List<String> operations, MultiMap<Integer, LogicalExpressionPlan> plans]
@init {
    $operations = new ArrayList<String>();
    $plans = new MultiMap<Integer, LogicalExpressionPlan>();
}
 : ( cube_rollup_list 
   { 
       $operations.add($cube_rollup_list.operation); 
       $plans.put( $cube_clause::inputIndex, $cube_rollup_list.plans); 
       $cube_clause::inputIndex++;
   } )+
;

cube_rollup_list returns[String operation, List<LogicalExpressionPlan> plans]
@init {
    $plans = new ArrayList<LogicalExpressionPlan>();
}
 : ^( ( CUBE { $operation = "CUBE"; } | ROLLUP { $operation = "ROLLUP"; } ) cube_by_expr_list { $plans = $cube_by_expr_list.plans; } ) 
;

cube_by_expr_list returns[List<LogicalExpressionPlan> plans]
@init {
    $plans = new ArrayList<LogicalExpressionPlan>();
}
 : ( cube_by_expr { $plans.add( $cube_by_expr.plan ); } )+
;

cube_by_expr returns[LogicalExpressionPlan plan]
@init {
    $plan = new LogicalExpressionPlan();
}
 : col_range[$plan]
 | expr[$plan]
 | STAR 
   {
       builder.buildProjectExpr( new SourceLocation( (PigParserNode)$STAR ), $plan, $GScope::currentOp, 0, null, -1 );
   }
;

group_clause returns[String alias]
scope {
    MultiMap<Integer, LogicalExpressionPlan> groupPlans;
    int inputIndex;
    List<String> inputAliases;
    List<Boolean> innerFlags;
}
scope GScope;
@init {
    $GScope::currentOp = builder.createGroupOp(); 
    $group_clause::groupPlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $group_clause::inputAliases = new ArrayList<String>();
    $group_clause::innerFlags = new ArrayList<Boolean>();
    GROUPTYPE groupType = GROUPTYPE.REGULAR;
    SourceLocation loc = new SourceLocation( (PigParserNode)$group_clause.start );
    int oldStatementIndex = $statement::inputIndex;
}
@after { $statement::inputIndex = oldStatementIndex; }
 : ^( GROUP group_item+ ( group_type { groupType = $group_type.type; ((LOCogroup)$GScope::currentOp).pinOption(LOCogroup.OPTION_GROUPTYPE); } )? partition_clause? )
   {
       $alias = builder.buildGroupOp( loc, (LOCogroup)$GScope::currentOp, $statement::alias, 
           $group_clause::inputAliases, $group_clause::groupPlans, groupType, $group_clause::innerFlags,
           $partition_clause.partitioner );
   }
 | ^( COGROUP group_item+ ( group_type { groupType = $group_type.type;((LOCogroup)$GScope::currentOp).pinOption(LOCogroup.OPTION_GROUPTYPE); } )? partition_clause? )
   {
       $alias = builder.buildGroupOp( loc, (LOCogroup)$GScope::currentOp, $statement::alias, 
           $group_clause::inputAliases, $group_clause::groupPlans, groupType, $group_clause::innerFlags,
           $partition_clause.partitioner );
   }
;

group_type returns[GROUPTYPE type]
 : QUOTEDSTRING
   {
       $type =builder.parseGroupType( $QUOTEDSTRING.text, new SourceLocation( (PigParserNode)$QUOTEDSTRING ) );
   } 
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
             ConstantExpression ce = new ConstantExpression( plan, "all");
             ce.setLocation( new SourceLocation( (PigParserNode)$ALL ) );
             List<LogicalExpressionPlan> plans = new ArrayList<LogicalExpressionPlan>( 1 );
             plans.add( plan );
             $group_clause::groupPlans.put( $group_clause::inputIndex, plans );
         }
         | ANY
         {
             LogicalExpressionPlan plan = new LogicalExpressionPlan();
             UserFuncExpression udf = new UserFuncExpression( plan, new FuncSpec( GFAny.class.getName() ) );
             udf.setLocation( new SourceLocation( (PigParserNode)$ANY ) );
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
 | inline_op
;

inline_op
@init {
    String al = $statement::alias;
    $statement::alias = null;
}
@after {
    $statement::alias = al;
}
 : op_clause parallel_clause?
   {
       Operator op = builder.lookupOperator( $op_clause.alias );
       builder.setParallel( (LogicalRelationalOperator)op, $statement::parallel );
       $statement::inputAlias = $op_clause.alias;
   }
;

flatten_generated_item returns[LogicalExpressionPlan plan, boolean flattenFlag, LogicalSchema schema]
@init {
    $plan = new LogicalExpressionPlan();
}
 : ( flatten_clause[$plan] { $flattenFlag = true; }
   | col_range[$plan]
   | expr[$plan]
   | STAR
     {
         builder.buildProjectExpr( new SourceLocation( (PigParserNode)$STAR ), $plan, $GScope::currentOp,
             $statement::inputIndex, null, -1 );
     }
   )
   ( field_def_list { $schema = $field_def_list.schema; } )?
;

flatten_clause[LogicalExpressionPlan plan]
 : ^( FLATTEN expr[$plan] )
;

store_clause returns[String alias]
 : ^( STORE rel filename func_clause[FunctionType.STOREFUNC]? )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$STORE );
       $alias= builder.buildStoreOp( loc, $statement::alias,
          $statement::inputAlias, $filename.filename, $func_clause.funcSpec );
   }
;

filter_clause returns[String alias]
scope GScope;
@init { 
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
    $GScope::currentOp = builder.createFilterOp();
}
 : ^( FILTER rel cond[exprPlan] )
   {
       $alias = builder.buildFilterOp( new SourceLocation( (PigParserNode)$FILTER ),
           (LOFilter)$GScope::currentOp, $statement::alias,
           $statement::inputAlias, exprPlan );
   }
;

cond[LogicalExpressionPlan exprPlan] returns[LogicalExpression expr]
 : ^( OR left = cond[exprPlan] right = cond[exprPlan] )
   {
       $expr = new OrExpression( $exprPlan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$OR ) );
   }
 | ^( AND left = cond[exprPlan] right = cond[exprPlan] )
   {
       $expr = new AndExpression( $exprPlan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$AND ) );
   }
 | ^( NOT c = cond[exprPlan] )
   {
       $expr = new NotExpression( $exprPlan, $c.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$NOT ) );
   }
 | ^( NULL expr[$exprPlan] NOT? )
   {
       $expr = new IsNullExpression( $exprPlan, $expr.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$NULL ) );
       if( $NOT != null ) {
           $expr = new NotExpression( $exprPlan, $expr );
           $expr.setLocation( new SourceLocation( (PigParserNode)$NOT ) );
       }
   }
 | ^( rel_op_eq e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new EqualExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_eq.start ) );
   } 
 | ^( rel_op_ne e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new NotEqualExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_ne.start ) );
   } 
 | ^( rel_op_lt e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new LessThanExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_lt.start ) );
   } 
 | ^( rel_op_lte e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new LessThanEqualExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_lte.start ) );
   }
 | ^( rel_op_gt e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new GreaterThanExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_gt.start ) );
   } 
 | ^( rel_op_gte e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new GreaterThanEqualExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$rel_op_gte.start ) );
   }
 | ^( STR_OP_MATCHES e1 = expr[$exprPlan] e2 = expr[$exprPlan] )
   {
       $expr = new RegexExpression( $exprPlan, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$STR_OP_MATCHES ) );
   }
 | func_eval[$exprPlan]
   {
       $expr = $func_eval.expr;
   }
 | ^( BOOL_COND e1 = expr[$exprPlan] )
   {
   	   $expr = $e1.expr;
       $expr.setLocation( new SourceLocation( (PigParserNode)$BOOL_COND ) );
   }   
;


func_eval[LogicalExpressionPlan plan] returns[LogicalExpression expr]
@init { 
    List<LogicalExpression> args = new ArrayList<LogicalExpression>();
}
 : ^( FUNC_EVAL func_name ( real_arg[$plan] { args.add( $real_arg.expr ); } )* )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$func_name.start );
       $expr = builder.buildUDF( loc, $plan, $func_name.funcName, args );
   }
;

real_arg [LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : e = expr[$plan] { $expr = $e.expr; }
 | STAR
   {
       $expr = builder.buildProjectExpr( new SourceLocation( (PigParserNode)$STAR ), $plan, $GScope::currentOp, 
           $statement::inputIndex, null, -1 );
   }
 | cr = col_range[$plan] { $expr = $cr.expr;}
;

expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( PLUS left = expr[$plan] right = expr[$plan] )
   {
       $expr = new AddExpression( $plan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$PLUS ) );
   }
 | ^( MINUS left = expr[$plan] right = expr[$plan] )
   {
       $expr = new SubtractExpression( $plan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$MINUS ) );
   }
 | ^( STAR left = expr[$plan] right = expr[$plan] )
   {
       $expr = new MultiplyExpression( $plan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$STAR ) );
   }
 | ^( DIV left = expr[$plan] right = expr[$plan] )
   {
       $expr = new DivideExpression( $plan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$DIV ) );
   }
 | ^( PERCENT left = expr[$plan] right = expr[$plan] )
   {
       $expr = new ModExpression( $plan, $left.expr, $right.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$PERCENT ) );
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
       $expr.setLocation( new SourceLocation( (PigParserNode)$e.start ) );
   }
 | ^( CAST_EXPR type_cast e = expr[$plan] ) // cast expr
   {
       $expr = new CastExpression( $plan, $e.expr, $type_cast.fieldSchema );
       $expr.setLocation( new SourceLocation( (PigParserNode)$type_cast.start ) );
   }
 | ^( EXPR_IN_PAREN e = expr[$plan] ) // unary expr
   {
       $expr = $e.expr;
   }
;

type_cast returns[LogicalFieldSchema fieldSchema]
 : simple_type
   {
        $fieldSchema = new LogicalFieldSchema( null, null, $simple_type.datatype );
   }
 | map_type
   {
       $fieldSchema = new LogicalFieldSchema( null, $map_type.logicalSchema, DataType.MAP );
   }
 | tuple_type_cast
   {
       $fieldSchema = new LogicalFieldSchema( null, $tuple_type_cast.logicalSchema, DataType.TUPLE );
   }
 | bag_type_cast
   {
       $fieldSchema = new LogicalFieldSchema( null, $bag_type_cast.logicalSchema, DataType.BAG );
   }
;

tuple_type_cast returns[LogicalSchema logicalSchema]
@init {
    $logicalSchema = new LogicalSchema();
}
 : ^( TUPLE_TYPE_CAST ( type_cast { $logicalSchema.addField( $type_cast.fieldSchema ); } )* )
;

bag_type_cast returns[LogicalSchema logicalSchema]
@init {
    $logicalSchema = new LogicalSchema();
}
 : ^( BAG_TYPE_CAST tuple_type_cast? )
   { 
       $logicalSchema.addField( new LogicalFieldSchema( null, $tuple_type_cast.logicalSchema, DataType.TUPLE ) );
   }
;

var_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
@init {
    List<Object> columns = null;
    SourceLocation loc = new SourceLocation( (PigParserNode)$var_expr.start );
}
 : projectable_expr[$plan] { $expr = $projectable_expr.expr; }
   ( dot_proj 
     {
         columns = $dot_proj.cols;
         boolean processScalar = false;
         if( $expr instanceof ScalarExpression ) {
             List<Operator> succs = plan.getSuccessors( $expr );
             if( succs == null || succs.size() == 0 ) {
                 // We haven't process this scalar projection yet. Set the flag so as to process it next.
                 // This will handle a projection such as A.u.x, where we need to build ScalarExpression
                 // for A.u, while for x, we need to treat it as a normal dereference (on the output of
                 // the ScalarExpression.
                 processScalar = true;
             }
         }
         
         if( processScalar ) {
             // This is a scalar projection.
             ScalarExpression scalarExpr = (ScalarExpression)$expr;
             
             if( $dot_proj.cols.size() > 1 ) {
                 throw new InvalidScalarProjectionException( input, loc, scalarExpr );
             }
             
             Object val = $dot_proj.cols.get( 0 );
             int pos = -1;
             LogicalRelationalOperator relOp = (LogicalRelationalOperator)scalarExpr.getImplicitReferencedOperator();
             LogicalSchema schema = null;
             try {
                 schema = relOp.getSchema();
             } catch(FrontendException e) {
                 throw new PlanGenerationFailureException( input, loc, e );
             }
             if( val instanceof Integer ) {
                 pos = (Integer)val;
                 if( schema != null && pos >= schema.size() ) {
                     throw new InvalidScalarProjectionException( input, loc, scalarExpr );
                 }
             } else {
                 String colAlias = (String)val;
                 pos = schema.getFieldPosition( colAlias );
                 if( schema == null || pos == -1 ) {
                     throw new InvalidScalarProjectionException( input, loc, scalarExpr );
                 }
             }
             
             ConstantExpression constExpr = new ConstantExpression( $plan, pos);
             plan.connect( $expr, constExpr );
             constExpr = new ConstantExpression( $plan, "filename"); // place holder for file name.
             plan.connect( $expr, constExpr );
         } else {
             DereferenceExpression e = new DereferenceExpression( $plan );
             e.setRawColumns( $dot_proj.cols );
             e.setLocation( new SourceLocation( (PigParserNode)$dot_proj.start ) );
             $plan.connect( e, $expr );
             $expr = e;
         }
     }
   | pound_proj
     {
         MapLookupExpression e = new MapLookupExpression( $plan, $pound_proj.key );
         e.setLocation( new SourceLocation( (PigParserNode)$pound_proj.start ) );
         $plan.connect( e, $expr );
         $expr = e;
     }
  )*
  {
      if( ( $expr instanceof ScalarExpression ) && columns == null ) {
          throw new InvalidScalarProjectionException( input, loc, (ScalarExpression)$expr, " : A column needs to be projected from a relation for it to be used as a scalar" );
      }
  }
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
 : GROUP { $col = $GROUP.text; }
 | CUBE { $col = $CUBE.text; }
 | IDENTIFIER { $col = $IDENTIFIER.text; }
;

col_index returns[Integer col]
 : DOLLARVAR { $col = builder.undollar( $DOLLARVAR.text ); }
;


col_range[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 :  ^(COL_RANGE (startExpr = col_ref[$plan])? DOUBLE_PERIOD (endExpr = col_ref[$plan])? )
    {
        SourceLocation loc = new SourceLocation( (PigParserNode)$col_range.start );
        $expr = builder.buildRangeProjectExpr(
                    loc, plan, $GScope::currentOp,
                    $statement::inputIndex, 
                    $startExpr.expr, 
                    $endExpr.expr
                );
    }  
;

pound_proj returns[String key]
 : ^( POUND ( QUOTEDSTRING { $key = builder.unquote( $QUOTEDSTRING.text ); } | NULL ) )
;

bin_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : ^( BIN_EXPR cond[$plan] e1 = expr[$plan] e2 = expr[$plan] )
   {
       $expr = new BinCondExpression( $plan, $cond.expr, $e1.expr, $e2.expr );
       $expr.setLocation( new SourceLocation( (PigParserNode)$bin_expr.start ) );
   }
;

limit_clause returns[String alias]
scope GScope;
@init { 
    $GScope::currentOp = builder.createLimitOp();
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
}
 :  ^( LIMIT rel ( INTEGER
   {
       $alias = builder.buildLimitOp( new SourceLocation( (PigParserNode)$LIMIT ), 
         $statement::alias, $statement::inputAlias, Long.valueOf( $INTEGER.text ) );
   }
 | LONGINTEGER
   {
       $alias = builder.buildLimitOp( new SourceLocation( (PigParserNode)$LIMIT ),
         $statement::alias, $statement::inputAlias, builder.parseLong( $LONGINTEGER.text ) );
   }
 | expr[exprPlan]
   {
       $alias = builder.buildLimitOp( new SourceLocation( (PigParserNode)$LIMIT ),
           (LOLimit)$GScope::currentOp, $statement::alias, $statement::inputAlias, exprPlan);
   }
 ) )
;

sample_clause returns[String alias]
scope GScope;
@init { 
    $GScope::currentOp = builder.createSampleOp();
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
}
 : ^( SAMPLE rel ( DOUBLENUMBER
   {
       $alias = builder.buildSampleOp( new SourceLocation( (PigParserNode)$SAMPLE ), $statement::alias,
           $statement::inputAlias, Double.valueOf( $DOUBLENUMBER.text ),
           new SourceLocation( (PigParserNode)$DOUBLENUMBER ) );
   }
 | expr[exprPlan]
   {
       $alias = builder.buildSampleOp( new SourceLocation( (PigParserNode)$SAMPLE ),
           (LOFilter)$GScope::currentOp, $statement::alias, $statement::inputAlias, exprPlan, $expr.expr);
   }
  ) )
;

order_clause returns[String alias]
scope GScope;
@init {
    $GScope::currentOp = builder.createSortOp();
}
 : ^( ORDER rel order_by_clause func_clause[FunctionType.COMPARISONFUNC]? )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$ORDER );
       $alias = builder.buildSortOp( loc, (LOSort)$GScope::currentOp, $statement::alias,
           $statement::inputAlias, $order_by_clause.plans, 
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
       builder.buildProjectExpr( new SourceLocation( (PigParserNode)$STAR ), plan, $GScope::currentOp,
           $statement::inputIndex, null, -1 );
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
 : col_range[$plan] (ASC | DESC { $ascFlag = false; } )?
 | col_ref[$plan] ( ASC | DESC { $ascFlag = false; } )?        
;

distinct_clause returns[String alias]
 : ^( DISTINCT rel partition_clause? )
   {
       $alias = builder.buildDistinctOp( new SourceLocation( (PigParserNode)$DISTINCT ), $statement::alias,
          $statement::inputAlias, $partition_clause.partitioner );
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
       $alias = builder.buildCrossOp( new SourceLocation( (PigParserNode)$CROSS ), $statement::alias,
          $rel_list.aliasList, $partition_clause.partitioner );
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
scope GScope;
@init {
    $GScope::currentOp = builder.createJoinOp();
    $join_clause::joinPlans = new MultiMap<Integer, LogicalExpressionPlan>();
    $join_clause::inputAliases = new ArrayList<String>();
    $join_clause::innerFlags = new ArrayList<Boolean>();
    int oldStatementIndex = $statement::inputIndex;
}
@after {
   $statement::inputIndex=oldStatementIndex;
}
 : ^( JOIN join_sub_clause join_type? partition_clause? )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$JOIN );
       $alias = builder.buildJoinOp( loc, (LOJoin)$GScope::currentOp, $statement::alias,
          $join_clause::inputAliases, $join_clause::joinPlans,
          $join_type.type, $join_clause::innerFlags, $partition_clause.partitioner );
   }
;

join_type returns[JOINTYPE type]
 : QUOTEDSTRING
   {
       $type = builder.parseJoinType( $QUOTEDSTRING.text, new SourceLocation( (PigParserNode)$QUOTEDSTRING ) );
   }
;

join_sub_clause
 : join_item ( LEFT { $join_clause::innerFlags.add( true ); 
                      $join_clause::innerFlags.add( false ); } 
             | RIGHT { $join_clause::innerFlags.add( false ); 
                       $join_clause::innerFlags.add( true ); }
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
 : col_range[$plan]
 | expr[$plan]
 | STAR 
   {
       builder.buildProjectExpr( new SourceLocation( (PigParserNode)$STAR ), $plan, $GScope::currentOp, 
           $statement::inputIndex, null, -1 );
   }
;

union_clause returns[String alias]
@init {
    boolean onSchema = false;
}
 : ^( UNION ( ONSCHEMA { onSchema = true; } )? rel_list )
   {
      $alias = builder.buildUnionOp( new SourceLocation( (PigParserNode)$UNION ), $statement::alias, 
          $rel_list.aliasList, onSchema );
   }
;

foreach_clause returns[String alias]
scope {
    LOForEach foreachOp;
}
scope GScope;
@init {
     $foreach_clause::foreachOp = builder.createForeachOp();
     $GScope::currentOp = $foreach_clause::foreachOp;
}
 : ^( FOREACH rel foreach_plan )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$FOREACH );
       $alias = builder.buildForeachOp( loc, $foreach_clause::foreachOp, $statement::alias,
          $statement::inputAlias, $foreach_plan.plan );
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
 : ^( FOREACH_PLAN_SIMPLE generate_clause )
 | ^( FOREACH_PLAN_COMPLEX nested_blk )
;

nested_blk : nested_command* generate_clause
;

nested_command
@init {
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
    inNestedCommand = true;
}
@after {
	inNestedCommand = false;
}
 : ^( NESTED_CMD IDENTIFIER nested_op[$IDENTIFIER.text] )
   {
       $foreach_plan::operators.put( $IDENTIFIER.text, $nested_op.op );
       $foreach_plan::exprPlans.remove( $IDENTIFIER.text );
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
 | nested_cross[$alias] { $op = $nested_cross.op; }
 | nested_foreach[$alias] { $op = $nested_foreach.op; }
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
       SourceLocation loc = new SourceLocation( (PigParserNode)$cr0.start );
       $op = builder.buildNestedProjectOp( loc, $foreach_plan::innerPlan, $foreach_clause::foreachOp, 
           $foreach_plan::operators, $alias, (ProjectExpression)$cr0.expr, plans );
   }
;

nested_filter[String alias] returns[Operator op]
scope GScope;
@init {
    LogicalExpressionPlan plan = new LogicalExpressionPlan();
    Operator inputOp = null;
    $GScope::currentOp = builder.createNestedFilterOp( $foreach_plan::innerPlan );
}
 : ^( FILTER nested_op_input cond[plan] )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$FILTER );
       $op = builder.buildNestedFilterOp( loc, (LOFilter)$GScope::currentOp, $foreach_plan::innerPlan, $alias, 
           $nested_op_input.op, plan );
   }
;

nested_sort[String alias] returns[Operator op]
scope GScope;
@init {
    Operator inputOp = null;
    $GScope::currentOp = builder.createNestedSortOp( $foreach_plan::innerPlan );
}
 : ^( ORDER nested_op_input order_by_clause func_clause[FunctionType.COMPARISONFUNC]? )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$ORDER );
       $op = builder.buildNestedSortOp( loc, (LOSort)$GScope::currentOp, $foreach_plan::innerPlan, $alias,
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
       SourceLocation loc = new SourceLocation( (PigParserNode)$DISTINCT );
       $op = builder.buildNestedDistinctOp( loc, $foreach_plan::innerPlan, $alias, $nested_op_input.op );
   }
;

nested_limit[String alias] returns[Operator op]
scope GScope;
@init {
    Operator inputOp = null;
    LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
    $GScope::currentOp = builder.createNestedLimitOp( $foreach_plan::innerPlan );
}
 : ^( LIMIT nested_op_input ( INTEGER 
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$LIMIT );
       $op = builder.buildNestedLimitOp( loc, $foreach_plan::innerPlan, $alias, $nested_op_input.op, 
           Integer.valueOf( $INTEGER.text ) );
   }
 | expr[exprPlan] 
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$LIMIT );
       $op = builder.buildNestedLimitOp( loc, (LOLimit)$GScope::currentOp, $foreach_plan::innerPlan, $alias,
           $nested_op_input.op, exprPlan);
   }
  ) )
;

nested_cross[String alias] returns[Operator op]
@init {
    Operator inputOp = null;
}
 : ^( CROSS nested_op_input_list )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$CROSS );
       $op = builder.buildNestedCrossOp( loc, $foreach_plan::innerPlan, $alias, $nested_op_input_list.opList );
   }
;

nested_foreach[String alias] returns[Operator op]
scope {
    LogicalPlan innerPlan;
    LOForEach foreachOp;
}
@init {
	Operator inputOp = null;
	$nested_foreach::innerPlan = new LogicalPlan();
	$nested_foreach::foreachOp = builder.createNestedForeachOp( $foreach_plan::innerPlan );
}
 : ^( FOREACH nested_op_input generate_clause )
   {
   		SourceLocation loc = new SourceLocation( (PigParserNode)$FOREACH );
   		$op = builder.buildNestedForeachOp( loc, (LOForEach)$nested_foreach::foreachOp, $foreach_plan::innerPlan,
   							$alias, $nested_op_input.op, $nested_foreach::innerPlan);
   }
;

generate_clause
scope GScope;
@init {
	$GScope::currentOp = builder.createGenerateOp(inNestedCommand ? $nested_foreach::innerPlan : $foreach_plan::innerPlan );
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
       builder.buildGenerateOp( new SourceLocation( (PigParserNode)$GENERATE ), 
       	   inNestedCommand ? $nested_foreach::foreachOp : $foreach_clause::foreachOp, 
           (LOGenerate)$GScope::currentOp, $foreach_plan::operators,
           plans, flattenFlags, schemas );
   }
;

nested_op_input returns[Operator op]
@init {
    LogicalExpressionPlan plan = new LogicalExpressionPlan();
}
 : col_ref[plan]
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$col_ref.start );
       $op = builder.buildNestedOperatorInput( loc, $foreach_plan::innerPlan,
           $foreach_clause::foreachOp, $foreach_plan::operators, $col_ref.expr );
   }
 | nested_proj[null]
   { 
       $op = $nested_proj.op;
   }
;

nested_op_input_list returns[List<Operator> opList]
@init { $opList = new ArrayList<Operator>(); }
 : ( nested_op_input { $opList.add( $nested_op_input.op ); } )+
;

stream_clause returns[String alias]
@init {
    StreamingCommand cmd = null;
    SourceLocation loc = new SourceLocation( (PigParserNode)$stream_clause.start );
}
 : ^( STREAM rel ( EXECCOMMAND { cmd = builder.buildCommand( loc, builder.unquote( $EXECCOMMAND.text ) ); } 
                 | IDENTIFIER 
                   { 
                       cmd = builder.lookupCommand( $IDENTIFIER.text );
                       if( cmd == null ) {
                           String msg = "Undefined command-alias [" + $IDENTIFIER.text + "]";
                           throw new ParserValidationException( input, 
                               new SourceLocation( (PigParserNode)$IDENTIFIER ), msg );
                       }
                   }
                 ) as_clause? )
   {
       $alias = builder.buildStreamOp( loc, $statement::alias,
          $statement::inputAlias, cmd, $as_clause.logicalSchema, input );
   }
;

mr_clause returns[String alias]
@init {
    List<String> paths = new ArrayList<String>();
    String alias = $statement::alias;
    SourceLocation loc = new SourceLocation( (PigParserNode)$mr_clause.start );
}
 : ^( MAPREDUCE QUOTEDSTRING path_list[paths]? 
     { $statement::alias = null; } store_clause 
     { $statement::alias = alias; } load_clause
     EXECCOMMAND? )
   {
       $alias = builder.buildNativeOp( loc,
           builder.unquote( $QUOTEDSTRING.text ), builder.unquote( $EXECCOMMAND.text ), 
           paths, $store_clause.alias, $load_clause.alias, input );
   }
;

split_clause
 : ^( SPLIT
      rel 
      { 
          SourceLocation loc = new SourceLocation( (PigParserNode)$SPLIT );
          $statement::inputAlias = builder.buildSplitOp( loc, $statement::inputAlias );
      } 
      split_branch+ split_otherwise?
    )
;

split_branch
scope GScope;
@init {
    LogicalExpressionPlan splitPlan = new LogicalExpressionPlan();
    $GScope::currentOp = builder.createSplitOutputOp();
}
 : ^( SPLIT_BRANCH alias cond[splitPlan] )
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$alias.start );
       builder.buildSplitOutputOp( loc, (LOSplitOutput)$GScope::currentOp, $alias.name,
           $statement::inputAlias, splitPlan );
   }
;

split_otherwise throws PlanGenerationFailureException
scope GScope;
@init {
    $GScope::currentOp = builder.createSplitOutputOp();
}
 : ^( OTHERWISE alias ) 
  {
       SourceLocation loc = new SourceLocation( (PigParserNode)$alias.start );
       builder.buildSplitOtherwiseOp( loc, (LOSplitOutput)$GScope::currentOp, $alias.name,
           $statement::inputAlias);
  }
;

col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : alias_col_ref[$plan] { $expr = $alias_col_ref.expr; }
 | dollar_col_ref[$plan] { $expr = $dollar_col_ref.expr; }
;

alias_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : GROUP 
   {
       $expr = builder.buildProjectExpr( new SourceLocation( (PigParserNode)$GROUP ), $plan, $GScope::currentOp, 
           $statement::inputIndex, $GROUP.text, 0 );
   }
 | CUBE 
   {
       $expr = builder.buildProjectExpr( new SourceLocation( (PigParserNode)$CUBE ), $plan, $GScope::currentOp, 
           $statement::inputIndex, $CUBE.text, 0 );
   }
 | IDENTIFIER
   {
       SourceLocation loc = new SourceLocation( (PigParserNode)$IDENTIFIER );
       String alias = $IDENTIFIER.text;
       Operator inOp = builder.lookupOperator( $statement::inputAlias );
       if(null == inOp)
       {
           throw new UndefinedAliasException (input,loc,$statement::inputAlias);
       }
       LogicalSchema schema;
       try {
           schema = ((LogicalRelationalOperator)inOp).getSchema();
       } catch (FrontendException e) {
           throw new PlanGenerationFailureException( input, loc, e );
       }
       
       Operator op = builder.lookupOperator( alias );
       if( op != null && ( schema == null || schema.getFieldPosition( alias ) == -1 ) ) {
           $expr = new ScalarExpression( plan, op,
               inForeachPlan ? $foreach_clause::foreachOp : $GScope::currentOp );
           $expr.setLocation( loc );
       } else {
           if( inForeachPlan ) {
               $expr = builder.buildProjectExpr( loc, $plan, $GScope::currentOp, 
                   $foreach_plan::exprPlans, alias, 0 );
           } else {
               $expr = builder.buildProjectExpr( loc, $plan, $GScope::currentOp, 
                   $statement::inputIndex, alias, 0 );
           }
       }
   }
;

dollar_col_ref[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : DOLLARVAR
   {
       int col = builder.undollar( $DOLLARVAR.text );
       $expr = builder.buildProjectExpr( new SourceLocation( (PigParserNode)$DOLLARVAR ), $plan, $GScope::currentOp, 
           $statement::inputIndex, null, col );
   }
;

const_expr[LogicalExpressionPlan plan] returns[LogicalExpression expr]
 : literal
   {
       $expr = new ConstantExpression( $plan, $literal.value);
       $expr.setLocation( new SourceLocation( (PigParserNode)$const_expr.start ) );
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
 : num_scalar
   {
       $type = $num_scalar.type;
       $value = $num_scalar.value;
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
 | TRUE
   {
       $type = DataType.BOOLEAN;
       $value = Boolean.TRUE;
   }
 | FALSE
   {
       $type = DataType.BOOLEAN;
       $value = Boolean.FALSE;
   }
;

num_scalar returns[Object value, byte type]
@init {
    int sign = 1;
}
 : ( MINUS { sign = -1; } ) ?
   ( INTEGER
     { 
         $type = DataType.INTEGER;
         $value = sign * Integer.valueOf( $INTEGER.text );
     }
   | LONGINTEGER 
     { 
         $type = DataType.LONG;
         $value = sign * builder.parseLong( $LONGINTEGER.text );
     }
   | FLOATNUMBER 
     { 
         $type = DataType.FLOAT;
         $value = sign * Float.valueOf( $FLOATNUMBER.text );
     }
   | DOUBLENUMBER 
     { 
         $type = DataType.DOUBLE;
         $value = sign * Double.valueOf( $DOUBLENUMBER.text );
     }
   )
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
    | IMPORT { $id = $IMPORT.text; }
    | RETURNS { $id = $RETURNS.text; }
    | DEFINE { $id = $DEFINE.text; }
    | LOAD { $id = $LOAD.text; }
    | FILTER { $id = $FILTER.text; }
    | FOREACH { $id = $FOREACH.text; }
    | MATCHES { $id = $MATCHES.text; }
    | ORDER { $id = $ORDER.text; }
    | DISTINCT { $id = $DISTINCT.text; }
    | COGROUP { $id = $COGROUP.text; }
    | CUBE { $id = $CUBE.text; }
    | ROLLUP { $id = $ROLLUP.text; }
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
    | BOOLEAN { $id = $BOOLEAN.text; }
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
    | TRUE { $id = $TRUE.text; }
    | FALSE { $id = $FALSE.text; }
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
    | TOBAG { $id = "TOBAG"; }
    | TOMAP { $id = "TOMAP"; }
    | TOTUPLE { $id = "TOTUPLE"; }
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
