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
                                                                                       
package TestDeployer;

###########################################################################
# Class: TestDeployer
# A base class for TestDeployer.  This interface defines a set of functions
# that can be called to deploy and undeploy resources for testing.  By default
# these methods are not called.  If the user adds -deploy to the command line
# then the following methods will be called on this interface, in the following
# order:
# 	checkPrerequisites()
#   deploy()
#   start()
#   generateData()
#   confirmDeployment()
# If the user adds -undeploy to the command line then the following methods 
# will be called on this inteface, in the following order:
#   deleteData()
#   stop()
#   undeploy()
#   confirmUndeployment()
#
# If either -deploy or -undeploy are invoked then -deploycfg <deply cfg file> must
# be supplied too.

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

##############################################################################
# Sub: checkPrerequisites
# Check any prerequisites before a deployment is begun.  For example if a 
# particular deployment required the use of a database system it could
# check here that the db was installed and accessible.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub checkPrerequisites
{
}

##############################################################################
# Sub: deploy
# Deploy any required packages
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub deploy
{
}

##############################################################################
# Sub: start
# Start any software modules that are needed.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub start
{
}

##############################################################################
# Sub: generateData
# Generate any data needed for this test run.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub generateData
{
}

##############################################################################
# Sub: confirmDeployment
# Run checks to confirm that the deployment was successful.  When this is 
# done the testing environment should be ready to run.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# Nothing
# This method should die with an appropriate error message if there is 
# an issue.
#
sub confirmDeployment
{
	die "$0 INFO : confirmDeployment is a virtual function!";
}

##############################################################################
# Sub: deleteData
# Remove any data created that will not be removed by undeploying.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub deleteData
{
}

##############################################################################
# Sub: stop
# Stop any servers or systems that are no longer needed once testing is
# completed.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub stop
{
}

##############################################################################
# Sub: undeploy
# Remove any packages that were installed as part of the deployment.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# None
#
sub undeploy
{
}

##############################################################################
# Sub: confirmUndeployment
# Run checks to confirm that the undeployment was successful.  When this is 
# done anything that must be turned off or removed should be turned off or
# removed.
#
# Paramaters:
# globalHash - hash from config file, including deployment config
# log - log file handle
#
# Returns:
# Nothing
# This method should die with an appropriate error message if there is 
# an issue.
#
sub confirmUndeployment
{
	die "$0 INFO : confirmUndeployment is a virtual function!";
}

1;








  
