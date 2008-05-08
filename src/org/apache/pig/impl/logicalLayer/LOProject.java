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
package org.apache.pig.impl.logicalLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * LOProject is designed like a singly linked list; A few examples will
 * illustrate the point about the linked list nature of the design;
 * a = load 'input1' as (name, age);
 * b = group a by name;
 * foreach b generate a, a.name;
 * The project operator occurs in two places in the above script:
 * generate a(here) and a.name(here)
 * In the first occurrence, we are trying to project the elements of
 * the bag a; In order to retrieve the bag, we need to project the
 * the second column ($1) or column number 1 (using the zero based index)
 * from the input (the relation or bag b)
 * In the second occurence, we are trying to project the first column
 * ($0) or column number 0 from the bag a which in turn is the column
 * number 1 in the relation b; As you can see, the nested structure or
 * the singly linked list nature is clearly visible;
 * Given that it's a singly linked list, the null pointer or the sentinel
 * is marked explictly using the boolean variable mSentinel; The sentinel
 * is marked true only when the input is a relational operator; This occurs
 * when we create the innermost operator
 */
public class LOProject extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The expression and the column to be projected.
     */
    private LogicalOperator mExp;
    private List<Integer> mProjection;
    private boolean mIsStar = false;
    private static Log log = LogFactory.getLog(LOProject.class);
	private boolean mSentinel;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the list of columns to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, LogicalOperator exp,
            List<Integer> projection) {
        super(plan, key);
        mExp = exp;
        mProjection = projection;
		if(mExp instanceof ExpressionOperator) {
			mSentinel = false;
		} else {
			mSentinel = true;
		}
    }

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the column to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, LogicalOperator exp,
            Integer projection) {
        super(plan, key);
        mExp = exp;
        mProjection = new ArrayList<Integer>(1);
        mProjection.add(projection);
		if(mExp instanceof ExpressionOperator) {
			mSentinel = false;
		} else {
			mSentinel = true;
		}
    }

    public LogicalOperator getExpression() {
        return mExp;
    }

    public List<Integer> getProjection() {
        return mProjection;
    }

    public int getCol() {
        if (mProjection.size() != 1)
            throw new RuntimeException(
                    "Internal error: improper use of getCol in "
                            + LOProject.class.getName());
        return mProjection.get(0);

    }

    public void setStar(boolean b) {
        mIsStar = b;
    }

    public boolean getSentinel() {
        return mSentinel;
    }

    public void setSentinel(boolean b) {
        mSentinel = b;
    }

    @Override
    public String name() {
        return "Project " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        log.debug("Inside getSchema");
		log.debug("Number of columns: " + mProjection.size());
        for (int i : mProjection) {
            log.debug("Column: " + i);
        }
		LogicalOperator expressionOperator = mExp;
		log.debug("expressionOperator = " + expressionOperator);
		log.debug("mIsStar: " + mIsStar);

        if (!mIsSchemaComputed && (null == mSchema)) {

            if (mIsStar) {
                log.debug("mIsStar is true");
                try {
                    if (null != expressionOperator) {
                        log.debug("expressionOperator is not null "
                                + expressionOperator.getClass().getName() + " " + expressionOperator);
                        mSchema = expressionOperator.getSchema();
                    }
                    mIsSchemaComputed = true;
                } catch (FrontendException ioe) {
                    mSchema = null;
                    mIsSchemaComputed = false;
                    throw ioe;
                }
                log.debug("mIsStar is true, returning schema of expressionOperator");
                log.debug("Exiting getSchema()");
                return mSchema;
            } else {
                List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                        mProjection.size());

                if (mProjection.size() == 1) {
                    log.debug("Only one element");
                    try {
                        Schema s = expressionOperator.getSchema();
                        if (null != s) {
							log.debug("Getting the fieldschema for column: " + mProjection.get(0));
                            Schema.FieldSchema fs = null; 
							if(!mSentinel) {
								log.debug("We are in the outer part of the projection");
								fs = s.getField(0);
								log.debug("fs in outer part: " + fs);
								s = fs.schema;
								log.debug("s: " + s);
								if(null != s) {
									fs = s.getField(mProjection.get(0));
									log.debug("fs in outer part after unwrapping: " + fs);
								} else {
									fs = null;
									log.debug("fs in outer part after unwrapping: " + fs);
								}
							} else {
								log.debug("We are in the inner most part of the projection");
								fs = s.getField(mProjection.get(0));
							}
                            if (null != fs) {
                                log.debug("fs.type: "
                                        + DataType.findTypeName(fs.type)
                                        + " fs.schema: " + fs.schema);
                                if (fs.type == DataType.BAG
                                        || fs.type == DataType.TUPLE) {
                                    if (null != fs.schema) {
                                        log.debug("fs.schema aliases");
                                        fs.schema.printAliases();
                                    }
                                    
                                    //mSchema = fs.schema;
                                    mSchema = new Schema(new Schema.FieldSchema(fs.alias, fs.schema));
                                    //mSchema = new Schema(fs);
                                } else {
                                    mSchema = new Schema(fs);
                                }
                            } else {
                            	log.debug("expressionOperator.schema is null");
                            	mSchema = new Schema(new Schema.FieldSchema(null,
                                    DataType.BYTEARRAY));
                            }
                        } else {
                            log.debug("expressionOperator.schema is null");
                            mSchema = new Schema(new Schema.FieldSchema(null,
                                    DataType.BYTEARRAY));
                        }
                    } catch (Exception e) {
                        mSchema = null;
                        mIsSchemaComputed = false;
                        throw new FrontendException(e.getMessage());
                    }
                    mIsSchemaComputed = true;
                    log.debug("Exiting getSchema");
                    return mSchema;
                }

                if (null != expressionOperator) {
                    log.debug("expressionOperator is not null");

                    Schema s = expressionOperator.getSchema();
                    log.debug("s: " + s);
                    for (int colNum : mProjection) {
                        log.debug("Col: " + colNum);
                        if (null != s) {
                            try {
                                Schema.FieldSchema tempFs = s.getField(0);
                                if (null != tempFs) {
                                    if(null != tempFs.schema) {
                                        Schema.FieldSchema fs = tempFs.schema.getField(colNum);
                                        log.debug("fs.type: "
                                                + DataType.findTypeName(fs.type)
                                                + " fs.schema: " + fs.schema);
                                        if(fs.type == DataType.BAG || fs.type == DataType.TUPLE) {
                                            if (null != fs.schema) {
                                                log.debug("fs.schema aliases");
                                                fs.schema.printAliases();
                                            }
                                                fss.add(new Schema.FieldSchema(fs.alias, fs.schema));
                                        } else {
                                            fss.add(fs);
                                        }
                                    } else {
                                        log.debug("expressionOperator.fs.schema is null");
                                        fss.add(new Schema.FieldSchema(null,
                                                DataType.BYTEARRAY));
                                    }
                                } else {
									log.debug("Could not find column " + colNum+ " in schema");
                                    throw new FrontendException(
                                            "Could not find column " + colNum
                                                    + " in schema");
                                }
                            } catch (Exception e) {
								log.debug("Caught exception: " + e.getMessage());
                                mSchema = null;
                                mIsSchemaComputed = false;
                                throw new FrontendException(e.getMessage());
                            }
                        } else {
                            log.debug("expressionOperator.schema is null");
                            fss.add(new Schema.FieldSchema(null,
                                    DataType.BYTEARRAY));
                        }
                    }
                } else {
                    log.debug("expressionOperator is null");
                    fss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
                }
                mSchema = new Schema(fss);
                mIsSchemaComputed = true;
                log.debug("mIsStar is false, returning computed schema of expressionOperator");
            }
        }

        log.debug("Exiting getSchema");
        return mSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
