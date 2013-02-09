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

-- The script converts simple.txt to simple table 

register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load 'simple.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray);

dump a1;

store a1 into 'simple-table' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f]');
a2 = load 'simple-table' using org.apache.hadoop.zebra.pig.TableLoader();
dump a2;

