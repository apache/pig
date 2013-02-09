/**
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

register /grid/0/dev/hadoopqa/hadoop/lib/zebra.jar;
A = load 'filter.txt' as (name:chararray, age:int);

B = filter A by age < 20;
--dump B;
store B into 'filter1' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

C = filter A by age >= 20;
--dump C;
Store C into 'filter2' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

