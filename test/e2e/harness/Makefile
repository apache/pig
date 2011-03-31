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
                                                                                       
PIG_TH_DIR     =	.

PIG_TH_MODULES =	$(PIG_TH_DIR)/Insert2Mysql.pm \
					$(PIG_TH_DIR)/TestDriverFactory.pm \
					$(PIG_TH_DIR)/TestDriver.pm  \
					$(PIG_TH_DIR)/TestReport.pm \
					$(PIG_TH_DIR)/Log.pm \
					$(PIG_TH_DIR)/Properties.pm
					 
PIG_TH_TEST_DIR		= $(PIG_TH_DIR)

PIG_TH_TEST_MODULES = $(PIG_TH_TEST_DIR)/TestDriverTest.pm

ARCHIVE = pigharness.tar

MAIN = pig_test_harness.pl

$(ARCHIVE): $(PIG_TH_MODULES) $(MAIN)
	tar cf $(ARCHIVE) $(PIG_TH_MODULES) $(MAIN)

TEST_DIR = testdist

test: $(ARCHIVE)
	mkdir -p $(TEST_DIR)
	mkdir -p $(TEST_DIR)/conf
	cp $(ARCHIVE) $(TEST_DIR)
	cp test/tests/test.conf $(TEST_DIR)
	cp test/conf/default.conf $(TEST_DIR)/conf
	(cd $(TEST_DIR); tar xf $(ARCHIVE))
	mkdir -p $(TEST_DIR)/$(PIG_TH_TEST_DIR)
	cp $(PIG_TH_TEST_MODULES) $(TEST_DIR)/$(PIG_TH_TEST_DIR)
	(cd $(TEST_DIR); export PIG_HARNESS_ROOT=.; ./$(MAIN) test.conf)

clean:
	rm -f $(ARCHIVE)
	rm -rf $(TEST_DIR)
