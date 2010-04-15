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
package org.apache.hadoop.owl.logical;

import java.util.List;
import java.util.Stack;

import org.apache.hadoop.owl.backend.BackendUtil;
import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.logical.CommandInfo;
import org.apache.hadoop.owl.logical.CommandInfo.Operator;
import org.apache.hadoop.owl.parser.OwlParser;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;

/**
 * The Class representing the filter as a tree. The tree has TreeNodes with two children and the leaf level nodes are of type LeafNode.
 */
public class ExpressionTree {

    /**
     * The Enum TreeOperator.
     */
    public enum TreeOperator {
        AND,
        OR
    }

    /**
     * The Class representing a Node in the ExpressionTree.
     */
    public static class TreeNode {
        public TreeNode lhs;
        public TreeOperator andOr;
        public TreeNode rhs;

        /**
         * Append the filter string to the buffer. Does a depth first traversal and prints the expression tree.
         * @param buffer the input buffer to append
         * @param backend the backend
         * @param otable the owl table
         * @param globalKeys the global keys
         * @throws OwlException the owl exception
         */
        public void appendFilterString(StringBuffer buffer, OwlBackend backend,
                OwlTableEntity otable, List<GlobalKeyEntity> globalKeys) throws OwlException {

            if( lhs != null ) {
                buffer.append(" (");

                lhs.appendFilterString(buffer, backend, otable, globalKeys);
                buffer.append(" " + andOr + " ");
                if( rhs != null ) {
                    rhs.appendFilterString(buffer, backend, otable, globalKeys);
                }
                buffer.append(")");
            } else if( rhs != null ) {
                throw new OwlException(ErrorType.ERROR_PARSING_FILTER);
            }            
        }
    }

    /**
     * The Class representing the leaf level nodes in the ExpressionTree.
     */
    public static class LeafNode extends TreeNode {
        public String keyName;
        public CommandInfo.Operator operator;
        public Object value;
        public boolean isReverseOrder = false;

        /**
         * Gets the value of the operator as a string value.
         * @return the string 
         */
        @SuppressWarnings("unchecked")
        String getValueString() {
            if( value instanceof List ) {

                //Handle list value (for in operator)
                List<? extends Object> list = (List<? extends Object>) value;
                StringBuffer buffer = new StringBuffer("(");
                boolean first = true;

                for(Object o : list) {
                    if( ! first  ) {
                        buffer.append(", ");
                    }

                    if( o instanceof String ) {
                        buffer.append("\"" + o.toString() + "\"");
                    } else {
                        buffer.append(o.toString());
                    }

                    first = false;
                }
                buffer.append(")");
                return buffer.toString();
            } else if (value instanceof String ){
                return "\"" + value + "\"";
            } else if (value instanceof Integer ){
                return value.toString();
            }

            return null;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.owl.logical.ExpressionTree.TreeNode#appendFilterString(java.lang.StringBuffer, org.apache.hadoop.owl.backend.OwlBackend, org.apache.hadoop.owl.entity.OwlTableEntity, java.util.List)
         */
        @Override
        @SuppressWarnings("unchecked")
        public void appendFilterString(StringBuffer buffer, OwlBackend backend,
                OwlTableEntity otable, List<GlobalKeyEntity> globalKeys) throws OwlException {

            KeyBaseEntity key = BackendUtil.getKeyForName(keyName, otable, globalKeys);

            if(! (key instanceof PartitionKeyEntity) ) {
                throw new OwlException(ErrorType.ERROR_UNKNOWN_PARTITION_KEY, keyName);
            }

            //Validate datatype of the value given for key
            DataType type = DataType.fromCode(key.getDataType());
            boolean validType = false;
            if( type == DataType.INT ) {
                if( value instanceof List ) {
                    if( ((List<? extends Object>) value).get(0) instanceof Integer ) {
                        validType = true;
                    } 
                } else if( value instanceof Integer ) {
                    validType = true;
                }
            } else if (type == DataType.STRING || type == DataType.LONG) {
                if( value instanceof List ) {
                    if( ((List<? extends Object>) value).get(0) instanceof String ) {
                        validType = true;
                    }
                } else if( value instanceof String ) {
                    validType = true;
                }
            }

            if( ! validType ) {
                //String value for integer or integer value for string or integer value for interval
                throw new OwlException(ErrorType.INVALID_FILTER_DATATYPE,
                        "Key <" + key.getName() + "> Value <" + getValueString() + ">");
            }

            Command.validateFilterOperator(key, operator);

            if( isReverseOrder ) {
                //For IN and LIKE, the value should be on the RHS 
                //For IN, currently grammar does not allow values on LHS, this check is for safety
                if( operator == Operator.IN || operator == Operator.LIKE ) {
                    throw new OwlException(ErrorType.INVALID_OPERATOR_ORDER,
                            "Key <" + key.getName() + "> Value <" + getValueString() + ">");
                }
            }


            //Create the string representation of the expression 
            buffer.append(" (");

            //Handle "a > 10" and "10 > a" appropriately 
            if( isReverseOrder ) {
                buffer.append(Command.getFilterForValue(key, operator, OwlParser.TrimQuotes(getValueString())));
            } else {
                buffer.append("[" + OwlUtil.toLowerCase( keyName ) + "]");
            }

            buffer.append(" " + Command.getFilterForOperator(key, operator, getValueString()).getOp() + " ");

            if( isReverseOrder ) {
                buffer.append("[" + OwlUtil.toLowerCase( keyName ) + "]");
            } else {
                buffer.append(Command.getFilterForValue(key, operator, OwlParser.TrimQuotes(getValueString())) );
            }

            buffer.append(")");
        }
    }

    /**
     * The root node for the tree.
     */
    private TreeNode root = null;

    /**
     * The node stack used to keep track of the tree nodes during parsing.
     */
    private Stack<TreeNode> nodeStack = new Stack<TreeNode>();

    /**
     * Adds a intermediate node of either type(AND/OR). Pops last two nodes from the stack and sets
     * them as children of the new node and pushes itself onto the stack. 
     * @param andOr the operator type
     */
    public void addIntermediateNode(TreeOperator andOr) {
        TreeNode newNode = new TreeNode();
        newNode.rhs = nodeStack.pop();
        newNode.lhs = nodeStack.pop();
        newNode.andOr = andOr;
        nodeStack.push(newNode);
        root = newNode;
    }

    /**
     * Adds a leaf node, pushes the new node onto the stack.
     * @param newNode the new node
     */
    public void addLeafNode(LeafNode newNode) {
        if( root == null ) {
            root = newNode;
        }
        nodeStack.push(newNode);
    }

    /**
     * Gets the filter as a string representation. Takes care of the required conversion in the operators, and handles
     * the square brackets around the partition keys as required by the backend.
     * @param backend the backend
     * @param otable the owl table
     * @param globalKeys the global keys
     * @return the filter string
     * @throws OwlException the owl exception
     */
    public String getFilterString(OwlBackend backend, OwlTableEntity otable, List<GlobalKeyEntity> globalKeys) throws OwlException {
        StringBuffer buffer = new StringBuffer();

        if( root != null ) {
            root.appendFilterString(buffer, backend, otable, globalKeys);
        }

        return OwlParser.TrimBrackets(buffer.toString().trim());
    }
}
