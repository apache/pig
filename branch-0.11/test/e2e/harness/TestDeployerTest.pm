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
                                                                                       
package TestDeployerTest;

use TestDeployer;

our @ISA = "TestDeployer";

use strict;

###########################################################################
# Class: TestDeployerTest
# A very simple test of TestDeployer.  It does nothing.

##############################################################################
# Sub: new
# Constructor
#
# Paramaters:
# None
#
# Returns:
# None.
sub new
{
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self = {};

	bless($self, $class);

	return $self;
}

sub confirmDeployment
{
}

sub confirmUndeployment
{
}

1;
