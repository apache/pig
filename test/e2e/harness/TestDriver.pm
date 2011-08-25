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
                                                                                       
package TestDriver;

###########################################################################
# Class: TestDriver
# A base class for TestDrivers.
# 

use TestDriverFactory;
use TestReport;
use File::Path;

my $passedStr = 'passed';
my $failedStr = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr = 'failed_dependency';


##############################################################################
#  Sub: printResults
#  Static function, can be used by test_harness.pl
#  Print the results so far, given the testStatuses hash.
#
# Paramaters:
# testStatuses - reference to hash of test status results.
# log    - reference to file handle to print results to.
# prefix - A title to prefix to the results
#
# Returns:
# None.
#
sub printResults
{
	my ($testStatuses, $log, $prefix) = @_;

	my ($pass, $fail, $abort, $depend, $skipped) = (0, 0, 0, 0, 0);

	foreach (keys(%$testStatuses)) {
		($testStatuses->{$_} eq $passedStr) && $pass++;
		($testStatuses->{$_} eq $failedStr) && $fail++;
		($testStatuses->{$_} eq $abortedStr) && $abort++;
		($testStatuses->{$_} eq $dependStr) && $depend++;
		($testStatuses->{$_} eq $skippedStr) && $skipped++;
	}

	my $msg = "$prefix, PASSED: $pass FAILED: $fail SKIPPED: $skipped ABORTED: $abort "
		. "FAILED DEPENDENCY: $depend";
	print $log "$msg\n";
 	print "$msg\n";
         
}

##############################################################################
#  Sub: printGroupResultsXml
#  Print the results for the group using junit xml schema using values from the testStatuses hash.
#
# Paramaters:
# $report       - the report object to use to generate the report
# $groupName    - the name of the group to report totals for
# $testStatuses - the hash containing the results for the tests run so far
# $totalDuration- The total time it took to run the group of tests
#
# Returns:
# None.
#
sub printGroupResultsXml
{
	my ( $report, $groupName, $testStatuses,  $totalDuration) = @_;
        $totalDuration=0 if  ( !$totalDuration );

	my ($pass, $fail, $abort, $depend) = (0, 0, 0, 0);

	foreach my $key (keys(%$testStatuses)) {
              if ( $key =~ /^$groupName/ ){
		($testStatuses->{$key} eq $passedStr) && $pass++;
		($testStatuses->{$key} eq $failedStr) && $fail++;
		($testStatuses->{$key} eq $abortedStr) && $abort++;
		($testStatuses->{$key} eq $dependStr) && $depend++;
               }
	}

        my $total= $pass + $fail + $abort;
        $report->totals( $groupName, $total, $fail, $abort, $totalDuration );

}

##############################################################################
#  Sub: new
# Constructor, should only be called by TestDriverFactory.
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

    $self->{'wrong_execution_mode'} = "_xyz_wrong_execution_mode_zyx_";

	return $self;
}

##############################################################################
#  Sub: globalSetup
# Set up before any tests are run.  This gives each individual driver a chance to do
# setup.  This function will only be called once, before all the tests are
# run.  A driver need not implement it.  It is a virtual function.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
#
sub globalSetup
{
}

###############################################################################
# Sub: globalCleanup
# Clean up after all tests have run.  This gives each individual driver a chance to do
# cleanup.  This function will only be called once, after all the tests are
# run.  A driver need not implement it.  It is a virtual function.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
sub globalCleanup
{
}

###############################################################################
# Sub: runTest
# Run a test.  This is a pure virtual function.
#
# Parameters:
# testcmd - reference to hash with meta tags and command to run tests.
# Interpretation of the tags and the command are up to the subclass.
# log - reference to a stream pointer for the logs
#
# Returns:
# @returns reference to hash.  Contents of hash are defined by the subclass.
#
sub runTest
{
	die "$0 INFO : This is a virtual function!";
}

###############################################################################
# Sub: generateBenchmark
# Generate benchmark results.  This is a pure virtual function.
#
# Parameters:
# benchmarkcmd - reference to hash with meta tags and command to
# generate benchmark.  Interpretation of the tags and the command are up to
# log - reference to a stream pointer for the logs
# the subclass.
#
# Returns:
# @returns reference to hash.  Contents of hash are defined by the subclass.
#
sub generateBenchmark
{
	die "$0 INFO: This is a virtual function!";
}

###############################################################################
# Sub: compare
# Compare the results of the test run with the generated benchmark results.  
# This is a pure virtual function.
#
# Parameters:
# benchmarkcmd - reference to hash with meta tags and command to
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# log - reference to a stream pointer for the logs
# testHash - reference to hash with meta tags and commands
#
# Returns:
# @returns reference true if results are the same, false otherwise.  "the
# same" is defined by the subclass.
#
sub compare
{
	die "$0 INFO: This is a virtual function!";
}

###############################################################################
# Sub: recordResults
# Record results of the test run.  The required fields are filled in by the
# test harness.  This call gives an individual driver a chance to fill in
# additional fields of cmd, cmd_id, expected_results, and actual_results.
# this function does not have to be implemened.
# This is a virtual function.
#
# Parameters:
# status - status of test passing, true or false
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# dbinfo - reference to hash that will be used to fill in db.
# log - reference to hash that will be used to fill in db.
#
# Returns:
# None
#
sub recordResults
{
}

###############################################################################
# Sub: cleanup
# Clean up after a test.  This gives each individual driver a chance to do
# cleanup.  A driver need not implement it.  It is a virtual function.
#
# Parameters:
# status - status of test passing, true or false
# testHash - reference to hash that was passed to runTest() and
# generateBenchmark().
# testResult - reference to hash returned by runTest.
# benchmarkResult - reference to hash returned by generateBenchmark.
# log - reference to a stream pointer for the logs
# 
# Returns:
# None
#
sub cleanup
{
}

###############################################################################
# Sub: run
# Run all the tests in the configuration file.
#
# Parameters:
# testsToRun - reference to array of test groups and ids to run
# testsToMatch - reference to array of test groups and ids to match.
# If a test group_num matches any of these regular expressions it will be run.
# cfg - reference to contents of cfg file
# log - reference to a stream pointer for the logs
# dbh - reference database connection
# testStatuses- reference to hash of test statuses
# confFile - config file name
# startat - test to start at.
# logname - name of the xml log for reporting results
#
# Returns:
# @returns nothing
# failed.
#
sub run
{
	my ($self, $testsToRun, $testsToMatch, $cfg, $log, $dbh, $testStatuses,
		$confFile, $startat, $logname ) = @_;

        my $subName      = (caller(0))[3];
        my $msg="";
        my $duration=0;
        my $totalDuration=0;
        my $groupDuration=0;
	my $sawstart = !(defined $startat);
	# Rather than make each driver handle our multi-level cfg, we'll flatten
	# the hashes into one for it.
	my %globalHash;

	my $runAll = ((scalar(@$testsToRun) == 0) && (scalar(@$testsToMatch) == 0));

	# Read the global keys
	foreach (keys(%$cfg)) {
		next if $_ eq 'groups';
		$globalHash{$_} = $cfg->{$_};
	}

	$globalHash{$_} = $cfg->{$_};
	# Do the global setup
	$self->globalSetup(\%globalHash, $log);

        my $report=0;
        my $properties= new Properties(0, $globalHash{'propertiesFile'});

        my %groupExecuted;
	foreach my $group (@{$cfg->{'groups'}}) {
 
                print $log "INFO $subName at ".__LINE__.": Running TEST GROUP(".$group->{'name'}.")\n";
                
		my %groupHash = %globalHash;
		$groupHash{'group'} = $group->{'name'};

		# Read the group keys
		foreach (keys(%$group)) {
			next if $_ eq 'tests';
			$groupHash{$_} = $group->{$_};
		}


		# Run each test
		foreach my $test (@{$group->{'tests'}}) {
			# Check if we're supposed to run this one or not.
			if (!$runAll) {
				# check if we are supposed to run this test or not.
				my $foundIt = 0;
				foreach (@$testsToRun) {
					if (/^$groupHash{'group'}(_[0-9]+)?$/) {
						if (not defined $1) {
							# In this case it's just the group name, so we'll
							# run every test in the group
							$foundIt = 1;
							last;
						} else {
							# maybe, it at least matches the group
							my $num = "_" . $test->{'num'};
							if ($num eq $1) {
								$foundIt = 1;
								last;
							}
						}
					}
				}
				foreach (@$testsToMatch) {
					my $protoName = $groupHash{'group'} . "_" .  $test->{'num'};
					if ($protoName =~ /$_/) {
						if (not defined $1) {
							# In this case it's just the group name, so we'll
							# run every test in the group
							$foundIt = 1;
							last;
						} else {
							# maybe, it at least matches the group
							my $num = "_" . $test->{'num'};
							if ($num eq $1) {
								$foundIt = 1;
								last;
							}
						}
					}
				}

				next unless $foundIt;
			}

			# This is a test, so run it.
			my %testHash = %groupHash;
			foreach (keys(%$test)) {
				$testHash{$_} = $test->{$_};
			}

			my $testName = $testHash{'group'} . "_" . $testHash{'num'};

#            if ( $groupExecuted{ $group->{'name'} }== 0 ){
#                $groupExecuted{ $group->{'name'} }=1;
#               
#                my $xmlDir= $globalHash{'localxmlpathbase'}."/run".$globalHash->{'UID'};
#                mkpath( [ $xmlDir ] , 1, 0777) if ( ! -e $xmlDir );
#
#                my $filename = $group->{'name'}.".xml";
#                $report = new TestReport ( $properties, "$xmlDir/$filename" );
#                $report->purge();
#            }

			# Check that ignore isn't set for this file, group, or test
			if (defined $testHash{'ignore'}) {
				print $log "Ignoring test $testName, ignore message: " .
					$testHash{'ignore'} . "\n";
				next;
			}

			# Have we not reached the starting point yet?
			if (!$sawstart) {
				if ($testName eq $startat) {
					$sawstart = 1;
				} else {
					next;
				}
			}

			# Check that this test doesn't depend on an earlier test or tests
			# that failed.  Don't abort if that test wasn't run, just assume the
			# user knew what they were doing and set it up right.
			my $skipThisOne = 0;
			foreach (keys(%testHash)) {
				if (/^depends_on/ && defined($testStatuses->{$testHash{$_}}) &&
						$testStatuses->{$testHash{$_}} ne $passedStr) {
					print $log "Skipping test $testName, it depended on " .
						"$testHash{$_} which returned a status of " .
						"$testStatuses->{$testHash{$_}}\n";
					$testStatuses->{$testName} = $dependStr;
					$skipThisOne = 1;
					last;
				}
			}
			if ($skipThisOne) {
				printResults($testStatuses, $log, "Results so far");
				next;
			}

			print $log "\n******************************************************\n";
			print $log "\nTEST: $confFile::$testName\n";
			print $log  "******************************************************\n";
			print $log "Beginning test $testName at " . time . "\n";
			my %dbinfo = (
				'testrun_id' => $testHash{'trid'},
				'test_type' => $testHash{'driver'},
				#'test_file' => $testHash{'file'},
				'test_file' => $confFile,
				'test_group' => $testHash{'group'},
				'test_num' => $testHash{'num'},
			);
			my $beginTime = time;
			my $endTime = 0;
			my ($testResult, $benchmarkResult);
			eval {
				$testResult = $self->runTest(\%testHash, $log);
				$endTime = time;
				$benchmarkResult = $self->generateBenchmark(\%testHash, $log);
				my $result =
					$self->compare($testResult, $benchmarkResult, $log, \%testHash);
				$msg = "INFO: $subName() at ".__LINE__.":Test $testName";

                if ($result eq $self->{'wrong_execution_mode'}) {
					$msg .= " SKIPPED";
					$testStatuses->{$testName} = $skippedStr;
                } elsif ($result) {
					$msg .= " SUCCEEDED";
					$testStatuses->{$testName} = $passedStr;

				} else {
					$msg .= " FAILED";
					$testStatuses->{$testName} = $failedStr;

				}
				$msg= "$msg at " . time . "\n";
				#print $msg;
				print $log $msg;
				$duration = $endTime - $beginTime;
				$dbinfo{'duration'} = $duration;
				$self->recordResults($result, $testResult
                                          , $benchmarkResult, \%dbinfo, $log);
                                  
			};


			if ($@) {
				$msg= "ERROR $subName at : ".__LINE__." Failed to run test $testName <$@>\n";
				#print $msg;
				print $log $msg;
				$testStatuses->{$testName} = $abortedStr;
				$dbinfo{'duration'} = $duration;
			}


			eval {
				$dbinfo{'status'} = $testStatuses->{$testName};
				if($dbh) {
					$dbh->insertTestCase(\%dbinfo);
				}
			};
			if ($@) {
				chomp $@;
				warn "Failed to insert test case info, error <$@>\n";
			}

			$self->cleanup($testStatuses->{$testName}, \%testHash, $testResult,
				$benchmarkResult, $log);
                        #$report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName}, $testResult ) if ( $report );
                        $report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName} ) if ( $report );
                        $groupDuration = $groupDuration + $duration;
                        $totalDuration = $totalDuration + $duration;
			printResults( $testStatuses, $log, "Results so far" );
		}

                if ( $report ) {
	           $report->systemOut( $logname, $group->{'name'});
	           printGroupResultsXml( $report, $group->{'name'}, $testStatuses, $groupDuration );
                }
                $report = 0;
                $groupDuration=0;


	}

	# Do the global cleanup
	$self->globalCleanup(\%globalHash, $log);
}

# TODO These should be removed

sub tmpIPCRun(){

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $log    = shift;
  my $msg    = shift;

  print $log "$0::$subName INFO Running ( @cmd )\n";

   my $result= `@cmd`;
   if ( $@ ) {
      my $msg= "$0::$subName FATAL Failed to run from $runningSubName $msg < $@ >\n$result\n";
      print $log $msg;
      die "$msg";
   }

   return $?;
}

sub tmpIPCRunSplitStdoe {

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $dir    = shift;
  my $log    = shift;
  my $msg    = shift;
  my $die    = shift;


  my $failed = 0;
  
  my $outfilename = $dir."out.tmp";
  my $errfilename = $dir."err.tmp";
  print $log "$0::$subName INFO Running from $runningSubName ( @cmd 1>$outfilename 2>$errfilename )\n";
  #make sure files are writeable
  open( TMP, ">$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for writing\n";
  close( TMP );
  open( TMP, ">$errfilename" ) || die "$0::$subName FATAL: Cannot open $errfilename for writing\n";
  close( TMP );

  #RUN CMD
  my $msg;
  print $log `@cmd 1>$outfilename 2>$errfilename`;
   
  my $failed=0;
  if ( $@ ) { 
      $msg= "$0::$subName FATAL < $@ >\n"; 
      $failed++;
   }
   
   #READ FILES
   my $stdout=""; 
   my $stderr="";;
   open( TMP, "$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for reading\n";
   while ( <TMP> ){ $stdout .= $_; }
   close( TMP );
 
   open( TMP, "$errfilename" ) || die "$0::$subName FATAL: Cannot open $errfilename for reading\n";
   while ( <TMP> ){
       $stderr .= $_;
   }
   close( TMP );

   #DIE IF Test Failed, otherwise return stdout and stderr
   if ( $failed ){

      $msg = "$0::$subName FATAL: Faied from $runningSubName \nSTDOUT:".$result{'stdout'}."\nSTDERR:".$result{'stderr'}."\n" if ( $failed );
      print $log "$msg";
      die $msg if ( $die != "1" ); #die by default
      return ( -1, $stdout, $stderr );

   }

   return ( $?, $stdout, $stderr);
}

sub tmpIPCRunJoinStdoe {

  my $self = shift;
  my $subName      = (caller(0))[3];
  my $runningSubName= shift;
  my $refCmd  = shift;
  my @cmd     = @$refCmd;
  my $outfilename= shift;
  my $log    = shift;
  my $msg    = shift;
  my $die    = shift;

  #make sure files are writeable
  open( TMP, ">$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for writing\n";
  close( TMP );

	  #RUN CMD
	  my $msg;
	  my $failed=0;
	  print $log "$0::$subName INFO Running ( @cmd 2>&1$outfilename 2>/dev/null )\n";
	  print $log `@cmd 2>&1 > $outfilename 2>/dev/null`;
	  if ( $@ ) { 
	      $failed++;
	      $msg= "$0::$subName FATAL < $@ >\n"; 
	   }
	   
	   #READ FILES
	   my $stdoe=""; 
	   open( TMP, "$outfilename" ) || die "$0::$subName FATAL: Cannot open $outfilename for reading\n";
	   while ( <TMP> ){ $stdoe .= $_; }
	   close( TMP );

	   if ( $failed ){
	      print $log "$msg";
	      die $msg if ( $die != "1" ); #die by default
	      return ( -1 ); 
	   }
	   return ( $? );
	}


	1;
