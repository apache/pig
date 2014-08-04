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
package org.apache.pig;

import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * A class to communicate Filter expressions to LoadFuncs.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Expression {

 // Operator type
    public static  enum OpType {

        // binary arith ops
        OP_PLUS (" + "),
        OP_MINUS(" - "),
        OP_TIMES(" * "),
        OP_DIV(" / "),
        OP_MOD(" % "),

        //binary ops
        OP_EQ(" == "),
        OP_NE(" != "),
        OP_GT(" > "),
        OP_GE(" >= "),
        OP_LT(" < "),
        OP_LE(" <= "),
        OP_MATCH(" matches "),

        //Only used by PredicatePushdown, not used by PartitionPushdown
        OP_IN (" in "),
        OP_BETWEEN (" between "),

        //unary ops
        OP_NULL(" is null"),
        OP_NOT(" not"),

        //binary logical
        OP_AND(" and "),
        OP_OR(" or "),
        TERM_COL(" Column "),
        TERM_CONST(" Constant ");

        private String str = "";
        private OpType(String rep){
            this.str = rep;
        }
        private OpType(){
        }

        @Override
        public String toString(){
            return this.str;
        }

    }

    protected OpType opType;

    /**
     * @return the opType
     */
    public OpType getOpType() {
        return opType;
    }

    //TODO: Apply a optimizer to Expression from PredicatePushdownOptimizer and
    // convert OR clauses to BETWEEN OR IN
    public static class BetweenExpression extends Expression {

        private Object lower;
        private Object upper;

        public BetweenExpression(Object lower, Object upper) {
            this.opType = OpType.OP_BETWEEN;
            this.lower = lower;
            this.upper = upper;
        }

        public Object getLower() {
            return lower;
        }

        public Object getUpper() {
            return upper;
        }

        @Override
        public String toString() {
            return " between " + lower + " and " + upper;
        }

    }

    public static class InExpression extends Expression {

        private List<Object> values;

        public InExpression(List<Object> values) {
            this.opType = OpType.OP_IN;
            this.values = values;
        }

        public List<Object> getValues() {
            return values;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(" in (");
            for (Object value : values) {
                if (value instanceof String) {
                    sb.append("'").append(value).append("', ");
                } else {
                    sb.append(value).append(", ");
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        }

    }

    public static class UnaryExpression extends Expression {

        Expression expr;

        public UnaryExpression(Expression expr, OpType opType) {
            this.opType = opType;
            this.expr = expr;
        }

        public Expression getExpression() {
            return expr;
        }

        @Override
        public String toString() {
            // TODO: Change toString() for OP_NOT to say (col is not null)
            // instead of ((col is null) not). If any one relies on expr.toString() might be useful
            return "(" + expr.toString() + opType.toString() + ")";
        }

    }

    public static class BinaryExpression extends Expression {

        /**
         * left hand operand
         */
        Expression lhs;

        /**
         * right hand operand
         */
        Expression rhs;


        /**
         * @param lhs
         * @param rhs
         */
        public BinaryExpression(Expression lhs, Expression rhs, OpType opType) {
            this.opType = opType;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        /**
         * @return the left hand operand
         */
        public Expression getLhs() {
            return lhs;
        }

        /**
         * @return the right hand operand
         */
        public Expression getRhs() {
            return rhs;
        }

        @Override
        public String toString() {
            return "(" + lhs.toString() + opType.toString() + rhs.toString()
                                + ")";
        }
    }

    public static class Column extends Expression {

        /**
         * name of column
         */
        private String name;

        /**
         * @param name
         */
        public Column(String name) {
            this.opType = OpType.TERM_COL;
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        /**
         * @return the name
         */
        public String getName() {
            return name;
        }

        /**
         * @param name the name to set
         */
        public void setName(String name) {
            this.name = name;
        }
    }

    public static class Const extends Expression {

        /**
         * value of the constant
         */
        Object value;

        /**
         * @return the value
         */
        public Object getValue() {
            return value;
        }

        /**
         * @param value
         */
        public Const(Object value) {
            this.opType = OpType.TERM_CONST;
            this.value = value;
        }

        @Override
        public String toString() {
            return (value instanceof String) ? "\'" + value + "\'":
                value.toString();
        }
    }

}


