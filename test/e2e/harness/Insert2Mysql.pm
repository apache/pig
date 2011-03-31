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
                                                                                       
package Insert2Mysql;
##############################################################################
#This package works with MySQL "testharness" database.
#It provides next functions:
#  * select list from DB - selectFromDB($query, $fieldsnames, $numfields);	
#  * insert info to testrun_results table - insertTestRun($test_type, $start_time, $end_time);
#  * insert info to testcases_results table - insertTestCase($testrun_id, $cols);
#  * update end_time for a test suite - updateEndTime($testrun_id, $end_time);
#  The constructor requires the database server and name be passed in.  The
#  database user will be assumed to be the logged in user, with password set
#  to login name.


use strict;
use DBI;

##############################################################################
# @param dbServer - hostname of database server
# @param dbDatabase - database name
sub new
{
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self = {};

	bless($self, $class);

	$self->{'dbServer'} = shift;
	$self->{'dbDatabase'} = shift;
	$self->{'dbUser'} = getlogin;

	$self->connect();

	return $self;
}

##############################################################################
# Need an explicit destructor to disconnect from the database.
sub DESTROY
{
	my $self = shift;

	# disconnect from database here
	$self->{'dbh'}->disconnect() if defined $self->{'dbh'};
}

##############################################################################
# Select a row from the database.
# @param query - query statement
# @param fieldsnames - reference to array of field names
# @param numfields - reference to var with number of fields
# @returns reference to array with arrays of field value
sub selectFromDB
{
	my ($self, $query, $fieldsnames, $numfields) = @_;	
	$self->checkDbConnection();

   	my $sth = $self->{'dbh'}->prepare($query);
	$sth->execute();

	# Fill out the pass by reference stuff.
	@$fieldsnames = @{$sth->{NAME}};
	$$numfields = $sth->{NUM_OF_FIELDS};
	
	# return the results.
	return $sth->fetchall_arrayref();
}

##############################################################################
# Record the beginning of a test run.
# @params testrun_desc - Description of test run.
# @returns testrun id created for this test
sub startTestRun
{
	my ($self, $testrun_desc) = @_;
	$self->checkDbConnection();

	# Insert into the table
	$self->{'dbh'}->do("insert into testrun_results (start_time, testrun_desc) values (now(), '$testrun_desc');")
		|| die ("Insert failed, $self->{'dbh'}->err");

	# Find out the testrun id we got.
	my $row = $self->{'dbh'}->selectrow_arrayref("select last_insert_id();");
	return $row->[0];
}

##############################################################################
# Record the ending of a test run.
# @param testrun_id - id o the test run that has been completed.
# @returns nothing
sub endTestRun
{
	my ($self, $testrun_id) = @_;
	$self->checkDbConnection();
	
   	# Update the table
	$self->{'dbh'}->do("update testrun_results set end_time = now() where testrun_id = $testrun_id;")
		|| die ("Insert failed, $self->{'dbh'}->err");
	# Update the dev_comment field inserting blank space
	$self->{'dbh'}->do("update testcase_results set dev_comment = ' ' where testrun_id = $testrun_id;")
		|| die ("Insert failed, $self->{'dbh'}->err");
}


##############################################################################
# Record the log file of a test run.
# @param testrun_id - id o the test run that has been completed.
# @param log_path - path to the log file
# @returns nothing
sub logTestRun
{
	my ($self, $testrun_id, $logpath) = @_;
	$self->checkDbConnection();
	
   	# insert a path to the log file
   	my $host = `hostname`;
   	chomp $host;
	$logpath = "$host/$logpath";
	$self->{'dbh'}->do("update testrun_results set log_path = '$logpath' WHERE testrun_id = $testrun_id;")
		|| die ("Insert failed, $self->{'dbh'}->err");
}


##############################################################################
# Record the results of executing a test case.
# @param results - reference to a hash.  The hash must have keys for
# testrun_id, test_type, test_file, test_group, test_num, status, and
# duration.  It may have entries for cmd, cmd_id, expected_results, and
# actual_results.  If these values have entries they will be inserted as well.
# @returns nothing
sub insertTestCase
{

	my ($self, $results) = @_;
	$self->checkDbConnection();

	# build statement with columns we know we'll need.
	my $query = "insert into testcase_results (testrun_id, test_type, test_file, test_group, test_num, status, duration";
	my $values = "values ($results->{testrun_id}, '$results->{test_type}',
	'$results->{test_file}', '$results->{test_group}', $results->{test_num},
	'$results->{status}', $results->{duration}";

	# Build in whathever option columns we find.
	foreach my $opt ('cmd', 'cmd_id', 'expected_results', 'actual_results') {
		if (defined $results->{$opt}) {
			$query .= ", $opt";
			$results->{$opt} =~ s/'/&#039;/g;
			$values .= ", '$results->{$opt}'";
		}
	}
	$query .= ")" . $values . ");";

	$self->{'dbh'}->do($query) || die ("Insert failed, $self->{'dbh'}->err");
} 

###################
# PRIVATE MEMBERS #
###################
##############################################################################
# Check that we are still connected to the database.  Intended only for usage
# by this class and it's children.
# @returns nothing
sub checkDbConnection
{
	my ($self) = @_;
	if (!$self->{'dbh'}->ping()) {
		$self->{'dbh'}->disconnect();
		$self->connect();
	}
}


##############################################################################
# Connect to the database
# @returns nothing
sub connect
{
	my ($self) = @_;
	$self->{'dbh'} =
		DBI->connect("DBI:mysql:$self->{dbDatabase}:$self->{dbServer}",
			$self->{'dbUser'}, "",
			{ RaiseError => 1, AutoCommit => 1 }) ||
			die ("Unable to connect to database");;
}


1;
