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

# Dockerfile for installing the necessary dependencies for building Apache Pig.
# See BUILDING.md.

FROM ubuntu:bionic

# Define working directory.
WORKDIR /root

RUN apt-get update

# Install dependencies from packages
RUN sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
    apt-get install -y build-essential && \
    apt-get install -y software-properties-common && \
    apt-get install --no-install-recommends -y \
            sudo \
            git subversion \
            byobu htop man unzip vim \
            cabal-install \
            curl wget \
            openjdk-8-jdk \
            ant ant-contrib ant-optional make maven \
            cmake gcc g++ protobuf-compiler \
            build-essential libtool \
            zlib1g-dev pkg-config libssl-dev \
            ubuntu-snappy ubuntu-snappy-cli libsnappy-dev \
            bzip2 libbz2-dev \
            libjansson-dev \
            fuse libfuse-dev \
            libcurl4-openssl-dev \
            python python2.7 && \
    rm -rf /var/lib/apt/lists/*

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Avoid out of memory errors in builds
ENV MAVEN_OPTS -Xms256m -Xmx512m

# Install findbugs
RUN mkdir -p /opt/findbugs && \
    wget http://sourceforge.net/projects/findbugs/files/findbugs/3.0.1/findbugs-noUpdateChecks-3.0.1.tar.gz/download \
         -O /opt/findbugs.tar.gz && \
    tar xzf /opt/findbugs.tar.gz --strip-components 1 -C /opt/findbugs
ENV FINDBUGS_HOME /opt/findbugs

# Install Forrest in /usr/local/apache-forrest
# Download
RUN cd /usr/local/ && wget "http://www.apache.org/dyn/closer.lua?action=download&filename=/forrest/apache-forrest-0.9-sources.tar.gz"      -O "apache-forrest-0.9-sources.tar.gz"
RUN cd /usr/local/ && wget "http://www.apache.org/dyn/closer.lua?action=download&filename=/forrest/apache-forrest-0.9-dependencies.tar.gz" -O "apache-forrest-0.9-dependencies.tar.gz"

# Unpack Apache Forrest
RUN cd /usr/local/ && \
    tar xzf apache-forrest-0.9-sources.tar.gz && \
    tar xzf apache-forrest-0.9-dependencies.tar.gz && \
    mv apache-forrest-0.9 apache-forrest
RUN cd /usr/local/apache-forrest/main && ./build.sh

# The solution for https://issues.apache.org/jira/browse/PIG-3906
RUN mkdir -p /usr/local/apache-forrest/plugins       && chmod a+rwX -R /usr/local/apache-forrest/plugins
RUN mkdir -p /usr/local/apache-forrest/build/plugins && chmod a+rwX -R /usr/local/apache-forrest/build/plugins

# Configure where forrest can be found
RUN echo 'forrest.home=/usr/local/apache-forrest' > build.properties
ENV FORREST_HOME /usr/local/apache-forrest

# Add a welcome message and environment checks.
ADD build_env_checks.sh /root/build_env_checks.sh
RUN chmod 755 /root/build_env_checks.sh
ADD configure-for-user.sh /root/configure-for-user.sh
RUN chmod 755 /root/configure-for-user.sh
RUN echo '~/build_env_checks.sh' >> /root/.bashrc
