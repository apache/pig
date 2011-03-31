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

TEST_DIR = ./tests

TESTS = $(TEST_DIR)/bigdata.conf  $(TEST_DIR)/cmdline.conf \
		$(TEST_DIR)/grunt.conf \
		$(TEST_DIR)/multiquery.conf  $(TEST_DIR)/negative.conf \
		$(TEST_DIR)/nightly.conf  $(TEST_DIR)/streaming.conf \
		$(TEST_DIR)/turing_jython.conf

CONF_DIR = ./conf

CONF_FILES = $(CONF_DIR)/default.conf

TOOL_DIR = ./tools

TEST_TOOL_DIR = $(TOOL_DIR)/test

GEN_TOOL_DIR = $(TOOL_DIR)/generate

TOOLS = $(TEST_TOOL_DIR)/explainchk.pl  $(TEST_TOOL_DIR)/floatpostprocessor.pl 

ARCHIVE = pigtests.tar

TEST_DIST_DIR = testdist

TESTS_TO_RUN ?= -t Checkin -t LoaderDefaultDir -t LoaderPigStorageArg \
			    -t FilterBoolean -t FilterEq -t FilterMatches -t FilterUdf \
			    -t CoGroupFlatten -t Join -t Distinct -t Cross -t Union \
			    -t Glob -t Arithmetic -t Regression -t Unicode -t Parameters \
			    -t ImplicitSplit -t describe -t MissingColumns -t Aliases \
				-t Lineage -t Casts -t ClassResolution \
			    -t MergeJoin -t SkewedJoin -t CollectedGroup -t Accumulator \
			    -t PruneColumns -t Bzip -t Scalar -t Scripting -t NestedDescribe \
				-t UdfDistributedCache -t Describe_cmdline -t Unicode_cmdline \
				-t Warning -t NoSuchFile -t BadFunc -t FileExists \
				-t NegForeach -t NotAllowed -t UdfException \
				-t LineageErrors -t ComputeSpec -t JobManagement \
				-t StreamingIO -t NonStreaming -t StreamingPerformance \
				-t RaceConditions -t StreamingLocal -t Jython_Checkin


			   
# Not yet working:
# LoaderBinStorage - results doctored
# LoaderTestLoader - results doctored
# GroupAggFunc     - results doctored
# EvalFunc         - results doctored
# CoGroup          - results doctored
# Foreach          - results doctored
# Order            - results doctored
# Bincond          - results doctored
# Types            - results doctored
# Limit            - results doctored
# Split            - results doctored
# SecondarySort    - results doctored
# Native           - results doctored
# Partitioner      - issues unclear
# CastScalar       - issues unclear
# udf_TOBAGandTOTUPLE -results doctored
# MergeOperator    - issues unclear
# Grunt            - needs reworked with different inputs
# MultiQuery       - results doctored
# SyntaxErrors     - needs streaming resources
# CmdErrors        - needs streaming resources
# StreamingErrors  - needs streaming resources
# IOErrors         - needs streaming resources
# StreamingLocalErrors - needs streaming resources
# Jython_*         - needs switch from java to bin/pig

UDF_DIR      = udfs

JAVA_UDF_DIR = $(UDF_DIR)/java

UDF_JAR      = $(JAVA_UDF_DIR)/testudf.jar

PYTHON_UDF_DIR = $(UDF_DIR)/python

PARAM_FILES  = paramfiles/params_3

LIB_DIR      = lib

STREAMING_DIR = streaming

$(ARCHIVE): $(TESTS) $(DRIVERS) $(CONF_FILES) $(TOOLS)
	tar cf $(ARCHIVE) $(DRIVERS) $(TESTS) $(CONF_FILES) $(TOOLS)

# Note, you must set PH_PIG_JARFILE environment variable
build_udfs: 
	(if [ "$${PH_PIG}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_PIG" \
		"before building the UDFs"; 1; fi)
	(cd $(JAVA_UDF_DIR); ant -Dpig.jarfile=$${PH_PIG}/pig.jar)

test: $(ARCHIVE) build_udfs
	(if [ "$${PH_CLUSTER}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_CLUSTER " \
		"before running the tests"; 1; fi)
	(if [ "$${PH_JYTHON_JAR}x" == "x" ] ; then echo \
	 	"You must set the environment variable PH_JYTHON_JAR " \
		"before running the tests"; 1; fi)
	mkdir -p $(TEST_DIST_DIR)/benchmarks
	cp $(ARCHIVE) $(TEST_DIST_DIR)
	cp ../harness/pigharness.tar $(TEST_DIST_DIR)
	(cd $(TEST_DIST_DIR); tar xf $(ARCHIVE))
	(cd $(TEST_DIST_DIR); tar xf pigharness.tar)
	(cd $(TEST_DIST_DIR); mv drivers/*.pm .)
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
	(cd $(TEST_DIST_DIR); PIG_HARNESS_ROOT=. PH_LOCAL=. PH_OUT=. PH_ROOT=. \
		./pig_test_harness.pl $(TESTS_TO_RUN) $(TESTS))

clean:
	rm -f $(ARCHIVE)
	rm -rf $(TEST_DIST_DIR)
	rm -rf $(JAVA_UDF_DIR)/build
	rm -rf $(JAVA_UDF_DIR)/testudf.jar


