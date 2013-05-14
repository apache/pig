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
                                                                                       
package ExistingClusterDeployer;

use IPC::Run qw(run);
use TestDeployer;

use strict;
use English;

our @ISA = "TestDeployer";

###########################################################################
# Class: ExistingClusterDeployer
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
    my ($self, $cfg, $log) = @_;

    # They must have declared the conf directory for their Hadoop installation
    if (! defined $cfg->{'hadoopconfdir'} || $cfg->{'hadoopconfdir'} eq "") {
        print $log "You must set the key 'hadoopconfdir' to your Hadoop conf directory "
            . "in existing_deployer.conf\n";
        die "hadoopconfdir is not set in existing_deployer.conf\n";
    }
    
    # They must have declared the executable path for their Hadoop installation
    if (! defined $cfg->{'hadoopbin'} || $cfg->{'hadoopbin'} eq "") {
        print $log "You must set the key 'hadoopbin' to your Hadoop bin path"
            . "in existing_deployer.conf\n";
        die "hadoopbin is not set in existing_deployer.conf\n";
    }

    # Run a quick and easy Hadoop command to make sure we can
    $self->runPigCmd($cfg, $log, "fs -ls /");

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
            'hdfs' => "singlefile/studenttab10k",
        }, {
            'name' => "votertab10k",
            'filetype' => "votertab",
            'rows' => 10000,
            'hdfs' => "singlefile/votertab10k",
        }, {
            'name' => "studentcolon10k",
            'filetype' => "studentcolon",
            'rows' => 10000,
            'hdfs' => "singlefile/studentcolon10k",
        }, {
            'name' => "textdoc",
            'filetype' => "textdoc",
            'rows' => 10000,
            'hdfs' => "singlefile/textdoc",
        }, {
            'name' => "reg1459894",
            'filetype' => "reg1459894",
            'rows' => 1000,
            'hdfs' => "singlefile/reg1459894",
        }, {
            'name' => "studenttabdir10k",
            'filetype' => "studenttab",
            'rows' => 10000,
            'hdfs' => "dir/studenttab10k",
        }, {
            'name' => "studenttabsomegood",
            'filetype' => "studenttab",
            'rows' => 1000,
            'hdfs' => "glob/star/somegood/studenttab",
        }, {
            'name' => "studenttabmoregood",
            'filetype' => "studenttab",
            'rows' => 1001,
            'hdfs' => "glob/star/moregood/studenttab",
        }, {
            'name' => "studenttabbad",
            'filetype' => "studenttab",
            'rows' => 1002,
            'hdfs' => "glob/star/bad/studenttab",
        }, {
            'name' => "fileexists",
            'filetype' => "studenttab",
            'rows' => 1,
            'hdfs' => "singlefile/fileexists",
        },{
            'name' => "nameMap",
            'filetype' => "studenttab",
            'rows' => 1,
            'hdfs' => "nameMap/part-00000",
        }, {
            'name' => "studenttab20m",
            'filetype' => "studenttab",
            'rows' => 20000000,
            'hdfs' => "singlefile/studenttab20m",
        }, {
            'name' => "unicode100",
            'filetype' => "unicode",
            'rows' => 100,
            'hdfs' => "singlefile/unicode100",
        }, {
            'name' => "studentctrla10k",
            'filetype' => "studentctrla",
            'rows' => 10000,
            'hdfs' => "singlefile/studentctrla10k",
        }, {
            'name' => "studentcomplextab10k",
            'filetype' => "studentcomplextab",
            'rows' => 10000,
            'hdfs' => "singlefile/studentcomplextab10k",
        }, {
            'name' => "studentnulltab10k",
            'filetype' => "studentnulltab",
            'rows' => 10000,
            'hdfs' => "singlefile/studentnulltab10k",
        }, {
            'name' => "voternulltab10k",
            'filetype' => "voternulltab",
            'rows' => 10000,
            'hdfs' => "singlefile/voternulltab10k",
        }, {
            'name' => "allscalar10k",
            'filetype' => "allscalar",
            'rows' => 10000,
            'hdfs' => "singlefile/allscalar10k",
        }, {
            'name' => "numbers.txt",
            'filetype' => "numbers",
            'rows' => 5000,
            'hdfs' => "types/numbers.txt",
        }, {
            'name' => "biggish",
            'filetype' => "biggish",
            'rows' => 1000000,
            'hdfs' => "singlefile/biggish",
        }, {
            'name' => "prerank",
            'filetype' => "ranking",
            'rows' => 30,
            'hdfs' => "singlefile/prerank",
        }
    );

	# Create the HDFS directories
	$self->runPigCmd($cfg, $log, "fs -mkdir $cfg->{'inpathbase'}");

    foreach my $table (@tables) {
		print "Generating data for $table->{'name'}\n";
		# Generate the data
        my @cmd = ("perl", $cfg->{'gentool'}, $table->{'filetype'}, $table->{'rows'},
            $table->{'name'});
		$self->runCmd($log, \@cmd);

		# Copy the data to HDFS
		my $hadoop = "copyFromLocal $table->{'name'} ".
			"$cfg->{'inpathbase'}/$table->{'hdfs'}";
		$self->runPigCmd($cfg, $log, $hadoop);

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
    # TODO: implement a correct confirmation, but let's not die there.
}

# TODO
# Need to rework this to take the Pig command instead of Hadoop.  That way
# it can use the existing utilities to build Pig commands and switch
# naturally to local mode with everything else.

sub runPigCmd($$$$)
{
    my ($self, $cfg, $log, $c) = @_;

    my @pigCmd = "";

    if ($cfg->{'usePython'} eq "true") {
      @pigCmd = ("$cfg->{'pigpath'}/bin/pig.py");
    } else {
      @pigCmd = ("$cfg->{'pigpath'}/bin/pig");
    }
    push(@pigCmd, '-e');
    push(@pigCmd, split(' ', $c));

    # set the PIG_CLASSPATH environment variable
    $ENV{'PIG_CLASSPATH'} = "$cfg->{'hadoopconfdir'}";
                          
    $self->runCmd($log, \@pigCmd);
}

sub runCmd($$$)
{
    my ($self, $log, $cmd) = @_;

    print $log "Going to run " . join(" ", @$cmd) . "\n";

    run($cmd, \undef, $log, $log) or
        die "Failed running " . join(" ", @$cmd) . "\n";
}

1;
