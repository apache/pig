#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

PIG_HOME=$bin/..

usage() {
  echo "
usage: $0 <parameters>

  Optional parameters:
    --conf-dir=/etc/pig              Set Pig configuration directory
    --hadoop-conf=/etc/hadoop        Set Hadoop configuration directory location
    --hadoop-home=/usr               Set Hadoop directory location
    --hbase-conf=/etc/hbase      Set HBase configuration directory location
    --hbase-home=/usr                Set HBase directory location
    --java-home=/usr/java/default    Set JAVA_HOME directory location
    --zookeeper-home=/usr            Set ZooKeeper directory location
  "
  exit 1
}

template_generator() {
  REGEX='(\$\{[a-zA-Z_][a-zA-Z_0-9]*\})'
  cat $1 |
  while read line ; do
    while [[ "$line" =~ $REGEX ]] ; do
      LHS=${BASH_REMATCH[1]}
      RHS="$(eval echo "\"$LHS\"")"
      line=${line//$LHS/$RHS}
    done
    echo $line >> $2
  done
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'conf-dir:' \
  -l 'hadoop-conf:' \
  -l 'hadoop-home:' \
  -l 'hbase-conf:' \
  -l 'hbase-home:' \
  -l 'java-home:' \
  -l 'zookeeper-home:' \
  -o 'h' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --conf-dir)
      PIG_CONF_DIR=$2
      shift 2
      ;;
    --hadoop-conf)
      HADOOP_CONF_DIR=$2
      shift 2
      ;;
    --hadoop-home)
      HADOOP_HOME=$2
      shift 2
      ;;
    --hbase-conf)
      HBASE_CONF_DIR=$2
      shift 2
      ;;
    --hbase-home)
      HBASE_HOME=$2
      shift 2
      ;;
    --java-home)
      JAVA_HOME=$2
      shift 2
      ;;
    --zookeeper-home)
      ZOOKEEPER_HOME=$2
      shift 2
      ;;
    --)
      shift ; break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

JAVA_HOME=${JAVA_HOME:-/usr/java/default}
HADOOP_HOME=${HADOOP_HOME:-/usr}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop}
HBASE_HOME=${HBASE_HOME:-/usr}
HBASE_CONF_DIR=${HBASE_CONF_DIR:-/etc/hbase}
ZOOKEEPER_HOME=${ZOOKEEPER_HOME:-/usr}
PIG_CONF_DIR=${PIG_CONF_DIR:-/etc/pig}

if [ -e ${HADOOP_HOME} ]; then
  # Check for Hadoop 0.20.2xx layout
  HADOOP_JAR=`ls ${HADOOP_HOME}/hadoop-core*.jar 2>/dev/null | head -n1`
  COMMONS_CONF_JAR=`ls ${HADOOP_HOME}/lib/commons-conf*.jar 2>/dev/null | head -n1`
  HADOOP_JAR=${HADOOP_JAR}:${COMMONS_CONF_JAR}
  if [ "x${HADOOP_JAR}" == "x" ]; then
    # Check for Hadoop 0.21 layout
    COMMON_JAR=`ls ${HADOOP_HOME}/hadoop-common*.jar 2>/dev/null | head -n1`
    if [ "x${COMMON_JAR}" != "x" ]; then
      HDFS_JAR=`ls ${HADOOP_HOME}/hadoop-hdfs*.jar 2>/dev/null | head -n1`
      MAPRED_JAR=`ls ${HADOOP_HOME}/hadoop-mapred*.jar 2>/dev/null | head -n1`
      HADOOP_JAR="${COMMON_JAR}:${HDFS_JAR}:${MAPRED_JAR}"
    else
      # Check for post Hadoop 0.23 layout
      COMMON_JAR=`ls ${HADOOP_HOME}/share/hadoop/common/hadoop-common*.jar 2>/dev/null | head -n1`
      if [ "x${HADOOP_JAR}" == "x" ]; then
        HDFS_JAR=`ls ${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs*.jar | head -n1`
        MAPRED_JAR=`ls ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapred*.jar | head -n1`
        HADOOP_JAR="${COMMON_JAR}:${HDFS_JAR}:${MAPRED_JAR}"
      fi
    fi
  fi
fi

if [ -e ${HBASE_HOME} ]; then
  HBASE_JAR=`ls ${HBASE_HOME}/hbase*.jar 2>/dev/null | head -n1`
  if [ "x${HBASE_JAR}" != "x" ]; then
    # Check for HBase 0.90 layout
    # Use HBase Bundled ZooKeeper Jar
    ZOOKEEPER_JAR=`ls ${HBASE_HOME}/lib/zookeeper*.jar 2>/dev/null | head -n1`
  elif [ -e ${HBASE_HOME}/share/hbase ]; then
    # Check for HBase 0.92 layout
    HBASE_JAR=`ls ${HBASE_HOME}/share/hbase/hbase-*.jar 2>/dev/null | head -n1`
    # Use HBase Bundled ZooKeeper Jar
    if [ -e ${HBASE_HOME}/share/hbase/lib ]; then
      ZOOKEEPER_JAR=`ls ${HBASE_HOME}/share/hbase/lib/zookeeper*.jar 2>/dev/null | head -n1`
    fi
  fi
fi

if [ -e ${HBASE_CONF_DIR} ]; then
  # Make HBase environment jar
  HBASE_CONF_JAR=${PIG_CONF_DIR}/hbase-env.jar
  ${JAVA_HOME}/bin/jar cf ${HBASE_CONF_JAR} -C ${HBASE_CONF_DIR} hbase-site.xml
  chmod 644 ${HBASE_CONF_JAR}
fi

if [ ! -e "${ZOOKEEPER_JAR}" ]; then
  # if ZOOKEEPER_JAR does not exist in HBase
  if [ "x${ZOOKEEPER_JAR}" == "x" ]; then
    # Check for ZooKeeper 3.3 layout
    ZOOKEEPER_JAR=`ls ${ZOOKEEPER_HOME}/zookeeper*.jar 2>/dev/null | head -n1`
  else
    # Check for ZooKeeper 3.4 layout
    ZOOKEEPER_JAR=`ls ${ZOOKEEPER_HOME}/share/zookeeper/zookeeper*.jar 2>/dev/null | head -n1`
  fi
fi

PIG_CLASSPATH=${HADOOP_CONF_DIR}:${HBASE_CONF_JAR}:${HADOOP_JAR}:${HBASE_JAR}:${ZOOKEEPER_JAR}

rm -f ${PIG_CONF_DIR}/pig-env.sh

template_generator ${PIG_HOME}/share/pig/templates/conf/pig-env.sh ${PIG_CONF_DIR}/pig-env.sh
chmod 755 ${PIG_CONF_DIR}/pig-env.sh

echo "Pig configuration setup is completed."
