############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

DRIVER_DIR = ./drivers

DRIVERS = $(DRIVER_DIR)/TestDriverPig.pm $(DRIVER_DIR)/Util.pm

DEPLOYER_DIR = ./deployers

DEPLOYERS = $(DEPLOYER_DIR)/ExistingClusterDeployer.pm

TEST_DIR = ./tests

TESTS = $(TEST_DIR)/cmdline.conf \
		$(TEST_DIR)/multiquery.conf  $(TEST_DIR)/negative.conf \
		$(TEST_DIR)/nightly.conf  $(TEST_DIR)/streaming.conf \
		$(TEST_DIR)/turing_jython.conf

# TODO Add in bigdata and macro tests.  bigdata should be in separate target

CONF_DIR = ./conf

CONF_FILES = $(CONF_DIR)/default.conf $(CONF_DIR)/existing_deployer.conf

TOOL_DIR = ./tools

TEST_TOOL_DIR = $(TOOL_DIR)/test

GEN_TOOL_DIR = $(TOOL_DIR)/generate

TOOLS = $(TEST_TOOL_DIR)/explainchk.pl  $(TEST_TOOL_DIR)/floatpostprocessor.pl \
        $(GEN_TOOL_DIR)/generate_data.pl

ARCHIVE = pigtests.tar

TEST_DIST_DIR ?= testdist

# TODO
# Make grunt tests works
# Make Jython tests work, since we don't have an old version for some of them
# to work aginst
# Make Macro tests work, can hand expand and test

# Not yet working:
# Jython_*         - needs switch from java to bin/pig

UDF_DIR      = udfs

JAVA_UDF_DIR = $(UDF_DIR)/java

UDF_JAR      = $(JAVA_UDF_DIR)/testudf.jar

PYTHON_UDF_DIR = $(UDF_DIR)/python

PARAM_FILES  = paramfiles/params_3

LIB_DIR      = lib

STREAMING_DIR = streaming

$(ARCHIVE): $(TESTS) $(DRIVERS) $(DEPLOYERS) $(CONF_FILES) $(TOOLS)
	tar cf $(ARCHIVE) $(DRIVERS) $(DEPLOYERS) $(TESTS) $(CONF_FILES) $(TOOLS)

# Note, you must set PH_PIG_JARFILE environment variable
build_udfs: 
	@(if [ "$${PH_PIG}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_PIG" \
		"to the directory your pig.jar is in " \
		"before building the UDFs"; 1; fi)
	(cd $(JAVA_UDF_DIR); ant -Dpig.jarfile=$${PH_PIG}/pig.jar)

pretest: $(ARCHIVE) build_udfs
	@(if [ "$${PH_OLDPIG}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_OLDPIG" \
		"to the directory your old version of Pig is installed in" \
		"before running the tests"; 1; fi)
	@(if [ "$${PH_CLUSTER}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_CLUSTER" \
		"to the directory that contains your hadoop-site.xml" \
		"before running the tests"; 1; fi)
	@(if [ "$${PH_JYTHON_JAR}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_JYTHON_JAR" \
		"to the path of your jython jar" \
		"before running the tests"; 1; fi)
	mkdir -p $(TEST_DIST_DIR)/benchmarks
	cp $(ARCHIVE) $(TEST_DIST_DIR)
	cp ../harness/pigharness.tar $(TEST_DIST_DIR)
	(cd $(TEST_DIST_DIR); tar xf $(ARCHIVE))
	(cd $(TEST_DIST_DIR); tar xf pigharness.tar)
	(cd $(TEST_DIST_DIR); mv drivers/*.pm .)
	(cd $(TEST_DIST_DIR); mv deployers/*.pm .)
	mkdir -p $(TEST_DIST_DIR)/libexec/PigTest
	(cd $(TEST_DIST_DIR); cp $(TOOLS) libexec/PigTest)
	chmod +x $(TEST_DIST_DIR)/libexec/PigTest/*
	cp $(CONF_DIR)/testpropertiesfile.conf $(TEST_DIST_DIR)/conf
	mkdir -p $(TEST_DIST_DIR)/lib/java
	cp $(UDF_JAR) $(TEST_DIST_DIR)/lib/java
	mkdir -p $(TEST_DIST_DIR)/libexec/python
	cp $(PYTHON_UDF_DIR)/*.py $(TEST_DIST_DIR)/libexec/python
	mkdir -p $(TEST_DIST_DIR)/paramfiles
	cp $(PARAM_FILES) $(TEST_DIST_DIR)/paramfiles
	cp $(LIB_DIR)/* $(TEST_DIST_DIR)/lib
	cp $(STREAMING_DIR)/* $(TEST_DIST_DIR)/libexec
	chmod +x $(TEST_DIST_DIR)/libexec/*.pl

test: pretest
	(cd $(TEST_DIST_DIR); PIG_HARNESS_ROOT=. PH_LOCAL=. PH_OUT=. PH_ROOT=. \
		./pig_test_harness.pl $(TESTS_TO_RUN) $(TESTS))

deployandtest: pretest
	(if [ "$${DEPLOYCFG}x" == "x" ] ; then echo \
	 	"You must set the environment variable DEPLOYCFG to the config file " \
		"for your deployment before running deploy"; 1; fi)
	(cd $(TEST_DIST_DIR); PIG_HARNESS_ROOT=. PH_LOCAL=. PH_OUT=. PH_ROOT=. \
		./pig_test_harness.pl -deploycfg $(DEPLOYCFG) -deploy $(TESTS_TO_RUN) \
		$(TESTS))


clean:
	rm -f $(ARCHIVE)
	rm -rf $(TEST_DIST_DIR)
	rm -rf $(JAVA_UDF_DIR)/build
	rm -rf $(JAVA_UDF_DIR)/testudf.jar


