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
                                                                                       
package LocalDeployer;

use IPC::Run qw(run);
use TestDeployer;

use strict;
use English;

our @ISA = "TestDeployer";

###########################################################################
# Class: LocalDeployer
# Deploy the Pig harness to a cluster and database that already exists.

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
# This is a no-op in this case because we're assuming both the cluster and the
# database already exist
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
# This is a no-op in this case because we're assuming both the cluster and the
# database already exist
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
    my ($self, $cfg, $log) = @_;
    my @tables = (
        {
            'name' => "studenttab10k",
            'filetype' => "studenttab",
            'rows' => 10000,
            'outfile' => "singlefile/studenttab10k",
        }, {
            'name' => "studenttab20m",
            'filetype' => "studenttab",
            'rows' => 20000000,
            'outfile' => "singlefile/studenttab20m",
        }, {
            'name' => "votertab10k",
            'filetype' => "votertab",
            'rows' => 10000,
            'outfile' => "singlefile/votertab10k",
        }, {
            'name' => "studentcolon10k",
            'filetype' => "studentcolon",
            'rows' => 10000,
            'outfile' => "singlefile/studentcolon10k",
        }, {
            'name' => "textdoc",
            'filetype' => "textdoc",
            'rows' => 10000,
            'outfile' => "singlefile/textdoc",
        }, {
            'name' => "reg1459894",
            'filetype' => "reg1459894",
            'rows' => 1000,
            'outfile' => "singlefile/reg1459894",
        }, {
            'name' => "studenttabdir10k",
            'filetype' => "studenttab",
            'rows' => 10000,
            'outfile' => "dir/studenttab10k",
        }, {
            'name' => "studenttabsomegood",
            'filetype' => "studenttab",
            'rows' => 1000,
            'outfile' => "glob/star/somegood/studenttab",
        }, {
            'name' => "studenttabmoregood",
            'filetype' => "studenttab",
            'rows' => 1001,
            'outfile' => "glob/star/moregood/studenttab",
        }, {
            'name' => "studenttabbad",
            'filetype' => "studenttab",
            'rows' => 1002,
            'outfile' => "glob/star/bad/studenttab",
        }, {
            'name' => "fileexists",
            'filetype' => "studenttab",
            'rows' => 1,
            'outfile' => "singlefile/fileexists",
        }, {
            'name' => "nameMap",
            'filetype' => "studenttab",
            'rows' => 1,
            'hdfs' => "nameMap/part-00000",
         },{
            'name' => "unicode100",
            'filetype' => "unicode",
            'rows' => 100,
            'outfile' => "singlefile/unicode100",
        }, {
            'name' => "studentctrla10k",
            'filetype' => "studentctrla",
            'rows' => 10000,
            'outfile' => "singlefile/studentctrla10k",
        }, {
            'name' => "studentcomplextab10k",
            'filetype' => "studentcomplextab",
            'rows' => 10000,
            'outfile' => "singlefile/studentcomplextab10k",
        }, {
            'name' => "studentnulltab10k",
            'filetype' => "studentnulltab",
            'rows' => 10000,
            'outfile' => "singlefile/studentnulltab10k",
        }, {
            'name' => "voternulltab10k",
            'filetype' => "voternulltab",
            'rows' => 10000,
            'outfile' => "singlefile/voternulltab10k",
        }, {
            'name' => "allscalar10k",
            'filetype' => "allscalar",
            'rows' => 10000,
            'outfile' => "singlefile/allscalar10k",
        }, {
            'name' => "numbers.txt",
            'filetype' => "numbers",
            'rows' => 5000,
            'outfile' => "types/numbers.txt",
        }, {
            'name' => "biggish",
            'filetype' => "biggish",
            'rows' => 1000000,
            'outfile' => "singlefile/biggish",
        }, {
            'name' => "prerank",
            'filetype' => "ranking",
            'rows' => 30,
            'outfile' => "singlefile/prerank",
        }
    );

	# Create the target directories
    for my $dir ("singlefile", "dir", "types", "glob/star/somegood",
            "glob/star/moregood", "glob/star/bad") {
        my @cmd = ("mkdir", "-p", "$cfg->{'inpathbase'}/$dir");
	    $self->runCmd($log, \@cmd);
    }

    foreach my $table (@tables) {
		print "Generating data for $table->{'name'}\n";
		# Generate the data
        my @cmd = ($cfg->{'gentool'}, $table->{'filetype'}, $table->{'rows'},
            $table->{'name'});
		$self->runCmd($log, \@cmd);

		# Move the data to its desitination
        my @cmd = ('mv', $table->{'name'},
            $cfg->{'inpathbase'} . "/" . $table->{'outfile'});
		$self->runCmd($log, \@cmd);

    }
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
}

sub runCmd($$$)
{
    my ($self, $log, $cmd) = @_;

    print $log "Going to run " . join(" ", @$cmd) . "\n";

    run($cmd, \undef, $log, $log) or
        die "Failed running " . join(" ", @$cmd) . "\n";
}

1
