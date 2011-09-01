#!/bin/bash

##########################################################################
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

OLD_PIG=0.8.1
ANT_VERSION=1.8.2
HADOOP_CONF=/etc/hadoop/conf
HADOOP_BIN=/usr/local/hadoop/bin/hadoop

while (( $# > 0 ))
do
    if [[ $1 = "-t" ]]
    then
        TESTS_TO_RUN="${TESTS_TO_RUN} -t $2"
        shift; shift
    elif [[ $1 == "-p" || $1 == "--patch" ]]
    then
        PATCH=$2
        shift; shift
    else
        echo Usage: $0 [-t testtorun ] [ -p patchfile ]
        echo "\t -t can be given multiple times"
        exit 1
    fi
done

if [[ -n ${TESTS_TO_RUN} ]]
then
    TESTS_TO_RUN="-Dtests.to.run=\"$TESTS_TO_RUN\""
fi

# Download ant and old version of Pig
mkdir tools
rc=$?
if (( $rc != 0 ))
then
    echo Failed to create tools directory
    exit $rc
fi

cd tools
wget http://archive.apache.org/dist//ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz
rc=$?
if (( $rc != 0 ))
then
    echo Failed to fetch ant tarball
    exit $rc
fi

tar zxf apache-ant-${ANT_VERSION}-bin.tar.gz 
rc=$?
if (( $rc != 0 ))
then
    echo Failed to untar ant tarball
    exit $rc
fi

wget http://archive.apache.org/dist//pig/pig-${OLD_PIG}/pig-${OLD_PIG}.tar.gz
rc=$?
if (( $rc != 0 ))
then
    echo Failed to fetch old pig tarball
    exit $rc
fi

tar zxf pig-${OLD_PIG}.tar.gz 
rc=$?
if (( $rc != 0 ))
then
    echo Failed to untar old pig tarball
    exit $rc
fi

# Fetch needed CPAN modules
cd ..
sudo cpan IPC::Run # need to find a way to make this headless
rc=$?
if (( $rc != 0 ))
then
    echo Failed to fetch IPC::Run
    exit $rc
fi

sudo cpan DBI
rc=$?
if (( $rc != 0 ))
then
    echo Failed to fetch DBI
    exit $rc
fi

# Fetch the source
mkdir src
rc=$?
if (( $rc != 0 ))
then
    echo Failed to create src directory
    exit $rc
fi

cd src
svn co http://svn.apache.org/repos/asf/pig/trunk 
rc=$?
if (( $rc != 0 ))
then
    echo Failed to checkout code from svn
    exit $rc
fi

cd trunk

if [[ -n ${PATCH} ]] 
then
    patch -p0 < ${PATCH}
    rc=$?
    if (( $rc != 0 ))
    then
        echo Failed to apply patch ${PATCH}
        exit $rc
    fi
fi

${HOME}/tools/apache-ant-${ANT_VERSION}/bin/ant -Dharness.old.pig=${HOME}/tools/pig-${OLD_PIG} -Dharness.cluster.conf=${HADOOP_CONF} -Dharness.cluster.bin=${HADOOP_BIN} test-e2e-deploy
${HOME}/tools/apache-ant-${ANT_VERSION}/bin/ant -Dharness.old.pig=${HOME}/tools/pig-${OLD_PIG} -Dharness.cluster.conf=${HADOOP_CONF} -Dharness.cluster.bin=${HADOOP_BIN} ${TESTS_TO_RUN} test-e2e


