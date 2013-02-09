/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Hadoop Table - tabular data storage for Hadoop MapReduce and PIG.
 * <p>
 * Hadoop Table provides tabular-type data storage for <a
 * href="http://hadoop.apache.org/core/docs/current/mapred_tutorial.html">Hadoop
 * MapReduce Framework</a>. It is also planned to allow Table to be closely
 * integrated with <a href="http://wiki.apache.org/pig/FrontPage">PIG</a>.
 * <p>
 * For this release, the basic construct of HadoopTable is called
 * {@link org.apache.hadoop.zebra.io.BasicTable}. A BasicTable is a create-once,
 * read-only kind of persisten data storage entity. A BasicTable contains zero
 * or more keyed rows.
 * <p>
 * The API uses Hadoop {@link org.apache.hadoop.io.BytesWritable} objects to
 * represent row keys, and PIG {@link org.apache.pig.data.Tuple} objects to
 * represent rows.
 * <p>
 * Each BasicTable maintains a {@link org.apache.hadoop.zebra.schema.Schema} ,
 * which, for this release, is nothing but a collection of column names. Given a
 * schema, we can deduce the integer index of a particular column, and use it to
 * extract (get) the desired datum from PIG Tuple object (which only allows
 * index-based access).
 * <p>
 * Typically, applications use
 * {@link org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat} (which implements
 * the Hadoop {@link org.apache.hadoop.mapred.OutputFormat} interface) to create
 * BasicTables through MapReduce. And they use
 * {@link org.apache.hadoop.zebra.mapreduce.TableInputFormat} (which implements the
 * Hadoop {@link org.apache.hadoop.mapred.InputFormat} to feed the data as their
 * MapReduce input.
 * <p>
 * The API is structured in three packages:
 * <UL>
 * <LI> {@link org.apache.hadoop.zebra.mapreduce} : The MapReduce layer. It contains
 * two classes: BasicTableOutputFormat for creating BasicTable; and
 * TableInputFormat for readding table.
 * 
 * <LI> {@link org.apache.hadoop.zebra.types} : Miscellaneous facilities that handle
 * column types and tuple serializations. Currently, it is a place holder that
 * redirects to PIG serialization. There is no type information being managed by
 * Table for individual columns.
 *
 * <LI> org.apache.hadoop.zebra.io : This is the internal IO layer. It deals 
 * with the physical storage (files) management of BasicTable. It also provides
 * facilities to help MapReduce layer create splits, such as partitioning
 * BasicTables for reading and reporting data block placement distributions
 * based on range-partitions or key-partitions.
 * </UL>
 */
package org.apache.hadoop.zebra;

