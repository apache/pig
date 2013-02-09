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
                                                                                       
package TestDeployerFactory;

###############################################################################
# Class: TestDeployerFactory
# A factory for TestDeployers.  This will read the environment and return the 
# correct TestDeployer.
#
 
use strict;

require Exporter;
our @ISA = "Exporter";
our @EXPORT_OK = qw(splitLine readLine isTag);

###############################################################################
# Sub: getTestDeployer
# Returns the appropriate Test Deployer.  This is determined by reading 
# the 'deployer' value in the config file.
#
# Returns:
# instance of appropriate subclass of TestDeployer
#
sub getTestDeployer
{
	my $cfg = shift;

	if (not defined $cfg->{'deployer'}) {
		die "$0 FATAL : I didn't see a deployer key in the file, I don't know "
			. "what deployer to instantiate.\n";
	}

	my $className = $cfg->{'deployer'};

    require "$className.pm";
	my $deployer = new $className();

	return $deployer;
}

1;
