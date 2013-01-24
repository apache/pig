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

register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
                      
a1order = order a1 by a,b;  
a2order = order a2 by a,b;   
       

c = foreach a1order  generate a,b;
d = foreach a2order  generate a,b;
store c into 'prune_ab12' using org.apache.hadoop.zebra.pig.TableStorer('[a,b]');    
store d into 'prune_ab22' using org.apache.hadoop.zebra.pig.TableStorer('[a,b]');

rec1 = load 'prune_ab12' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'prune_ab22' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by (a,b), rec2 by (a,b) using "merge" ;     

dump joina;
--table merge1 should only have 8 columns from left hand, 2 columns from right hand. 
store joina into 'merge2' using org.apache.hadoop.zebra.pig.TableStorer('');


             
