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

rmf input-copy.txt; cat 'foo'; a = load '1.txt' ; aliases;illustrate a; copyFromLocal foo bar; copyToLocal foo bar; describe a; mkdir foo; run bar.pig; exec bar.pig; cp foo bar; explain a;cd 'bar'; pwd; ls ; fs -ls ; fs -rmr foo; mv foo bar; dump a;store a into 'input-copy.txt' ; a = load '2.txt' as (b);explain a; rm foo; store a into 'bar';
