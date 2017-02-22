#!/bin/bash

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

set -e               # exit on error

cd "$(dirname "$0")" # connect to root

docker build -t pig-build dev-support/docker

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=${USER}}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=${USER}
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "pig-build-${USER_NAME}" - <<UserSpecificDocker
FROM pig-build
RUN bash configure-for-user.sh ${USER_NAME} ${USER_ID} ${GROUP_ID} "$(fgrep vboxsf /etc/group)"
UserSpecificDocker

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system. This also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
# Same with the .ivy2 directory

DOCKER="docker run --rm=true -t -i"
DOCKER=${DOCKER}" -u ${USER_NAME}"

# Work in the current directory
DOCKER=${DOCKER}" -v ${PWD}:/home/${USER_NAME}/pig"
DOCKER=${DOCKER}" -w /home/${USER_NAME}/pig"

# Mount persistent caching of 'large' downloads
DOCKER=${DOCKER}" -v ${HOME}/.m2:/home/${USER_NAME}/.m2"
DOCKER=${DOCKER}" -v ${HOME}/.ivy2:/home/${USER_NAME}/.ivy2"

# What do we run?
DOCKER=${DOCKER}" --name pig-build-${USER_NAME}-$$"
DOCKER=${DOCKER}" pig-build-${USER_NAME}"
DOCKER=${DOCKER}" bash"

# Now actually start it
${DOCKER}

