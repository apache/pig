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

# This script configures pig-env.sh and symlinkis directories for 
# relocating package locations.

usage() {
  echo "
usage: $0 <parameters>
  Required parameters:
     --prefix=PREFIX             path to install into

  Optional parameters:
     --arch=i386                 OS Architecture
     --bin-dir=PREFIX/bin        Executable directory
     --conf-dir=/etc/pig         Configuration directory
     --log-dir=/var/log/pig      Log directory
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
  -l 'arch:' \
  -l 'prefix:' \
  -l 'bin-dir:' \
  -l 'conf-dir:' \
  -l 'lib-dir:' \
  -l 'log-dir:' \
  -l 'uninstall' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --arch)
      ARCH=$2 ; shift 2
      ;;
    --prefix)
      PREFIX=$2 ; shift 2
      ;;
    --bin-dir)
      BIN_DIR=$2 ; shift 2
      ;;
    --log-dir)
      LOG_DIR=$2 ; shift 2
      ;;
    --lib-dir)
      LIB_DIR=$2 ; shift 2
      ;;
    --conf-dir)
      CONF_DIR=$2 ; shift 2
      ;;
    --uninstall)
      UNINSTALL=1; shift
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

for var in PREFIX; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

ARCH=${ARCH:-i386}
BIN_DIR=${BIN_DIR:-$PREFIX/bin}
CONF_DIR=${CONF_DIR:-$PREFIX/etc/pig}
LIB_DIR=${LIB_DIR:-$PREFIX/share/pig}
LOG_DIR=${LOG_DIR:-$PREFIX/var/log}
UNINSTALL=${UNINSTALL:-0}
PIG_HOME=${PREFIX}

if [ "${ARCH}" != "i386" ]; then
  LIB_DIR=${LIB_DIR}64
fi

. /etc/default/hadoop-env.sh

if [ -e /etc/default/hbase-env.sh ]; then
  . /etc/default/hbase-env.sh
fi

if [ "${UNINSTALL}" -eq "1" ]; then
  # Remove symlinks
  if [ "${CONF_DIR}" != "${PREFIX}/etc/pig" ]; then
    rm -f ${PREFIX}/etc/pig
  fi
else
  # Create symlinks
  if [ "${CONF_DIR}" != "${PREFIX}/etc/pig" ]; then
    if [ ! -e ${PREFIX}/etc/pig ]; then
      ln -sf ${CONF_DIR} ${PREFIX}/etc/pig
    fi
  fi

  mkdir -p ${LOG_DIR}
  chown root:hadoop ${LOG_DIR}
  chmod 777 ${LOG_DIR}

  TFILE="/tmp/$(basename $0).$$.tmp"
  if [ -z "${JAVA_HOME}" ]; then
    if [ -e /etc/lsb-release ]; then
      JAVA_HOME=`update-alternatives --config java | grep java | cut -f2 -d':' | cut -f2 -d' ' | sed -e 's/\/bin\/java//'`
    else
      JAVA_HOME=/usr/java/default
    fi
  fi
  template_generator ${PREFIX}/share/pig/templates/conf/pig-env.sh $TFILE
  cp ${TFILE} ${CONF_DIR}/pig-env.sh
  rm -f ${TFILE}
fi
