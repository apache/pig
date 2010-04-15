#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -x
if [ -n "$4" ]
then
    PORT=$4
else
    PORT=8080
fi

if test `uname` == "Darwin" 
then
    if test "" == "$JAVA_OPTS"
    then
        export JAVA_OPTS="-Djava.io.tmpdir=/tmp"
    fi    
fi

echo jetty_start war: $1 path: $2 ci.dir: $3 port: $PORT
java $JAVA_OPTS -Dorg.apache.hadoop.owl.xmlconfig=$3/../setup/derby/owlServerConfig.xml -jar $3/jetty-runner-7.0.0.pre5.jar --path $2 --port $PORT --log $3/owljetty.log $1 > $3/owljetty.log 2>&1 &
