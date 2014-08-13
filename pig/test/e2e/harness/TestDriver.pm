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
use Parallel::ForkManager;
use FileHandle;
use File::Copy;
use File::Basename;

my $passedStr = 'passed';
my $failedStr = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr = 'failed_dependency';

# A constant to be used as a key in hash:
my $keyGlobalSetupConditionalDone = 'globalSetupConditionaldone';

################################################################################
# Sub: appendToLength
# static mathod to make a string not shorter than N characters in length: 
# appends spaces unlit the desired length achieved.
#
# Paramaters:
# str - the string
# len - the min deired length 
#
# Returns: the modified string
#
sub appendToLength($$) {
   my ($str, $len) = @_; 
   while (length($str) < $len) {
      $str .= " ";
   }   
   return $str;
}

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
	my ($testStatuses, $log, $prefix, $confFile, $groupName) = @_;

	my ($pass, $fail, $abort, $depend, $skipped) = (0, 0, 0, 0, 0);

	foreach (keys(%$testStatuses)) {
		($testStatuses->{$_} eq $passedStr) && $pass++;
		($testStatuses->{$_} eq $failedStr) && $fail++;
		($testStatuses->{$_} eq $abortedStr) && $abort++;
		($testStatuses->{$_} eq $dependStr) && $depend++;
		($testStatuses->{$_} eq $skippedStr) && $skipped++;
	}

        my $context = "";
        if (defined($confFile) && (length(confFile) > 0)) {
           $context = " [$confFile-$groupName]"; 
        } 
        # XXX why comma is added there?
	my $msg = appendToLength($prefix . ",", 18) 
            . " PASSED: " . appendToLength($pass,4) 
            . " FAILED: " . appendToLength($fail,4) 
            . " SKIPPED: " . appendToLength($skipped,4) 
            . " ABORTED: " . appendToLength($abort,4) 
            . " FAILED DEPENDENCY: " . appendToLength($depend,4)
            . $context;
	print $log "$msg\n";
 	print "$msg\n";      
}

##############################################################################
# Puts all the k-v pairs from sourceHash to the targetHash.
# Returns: void
# parameters: 
#   1: targetHash, 
#   2: sourceHash.
sub putAll($$)
{
    my ($targetHash, $sourceHash) = @_;
    while (my ($key, $value) = each(%$sourceHash)) {
        $targetHash->{ $key } = $value;
    }
}

##############################################################################
# appends one file to another.
# parameters: 
#   1: sourceFileName, 
#   2: targetFileName.
# Returns: void
sub appendFile($$) {
    my ($source, $target) = @_;
    dbg("Appending file [" . Cwd::realpath($source) . "] >> [" . Cwd::realpath($target) . "]\n");
    $sourceHandle = FileHandle->new("<$source");
    if (! defined $sourceHandle) {
        die "Cannot open source file [$source].";
    }
    $targetHandle = FileHandle->new(">>$target");
    if (defined $targetHandle) {
        copy($sourceHandle, $targetHandle);
        $targetHandle->close(); 
        $sourceHandle->close(); 
    } else {
        die "cannot open target file [$target].";
    }
}

##############################################################################
# Diagnostic sub to print a hash contents
# Paramaters: 
#   1: the hash reference;
# Returns: void
sub dbgDumpHash($;$)
{
    if ($ENV{'E2E_DEBUG'} eq 'true') {
       my ($myhash, $msg) = @_;
       print "Dump of hash $msg:\n";
       while (my ($key, $value) = each(%$myhash)) {
           print "  [$key] = [$value]\n";
       }
    }
}

##############################################################################
# Diagnostic sub to print a debug output.
# This is useful when debugging the harness perl scripts.
# Paramaters: 
#   1*: object(s) to be printed, typically one string;
# Returns: void
sub dbg(@) 
{
    if ($ENV{'E2E_DEBUG'} eq 'true') {
       print @_;
    }
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
# Set up before any tests from the group are run.  This gives each individual driver a chance to do
# setup.  This function will only be called once, before all the tests are
# run.  A driver need not implement it.  It is a virtual function.
#
# This method invoked unconditionally (always), even if there are no test to run in the group. See
# also #globalSetupConditional() description. 
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

##############################################################################
#  Sub: globalSetupConditional 
# Set up before any tests from the test config file (in sequential mode) or test group (in parallel mode) are run. 
# Executes after #globalSetup(). Executes *only* if there is at least one test to run. Introduced for performance
# optimization in parallel execution mode.
# It is a virtual function. Subclasses may override it.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
#
sub globalSetupConditional
{
}

###############################################################################
# Sub: globalCleanup
# Clean up after all tests have run.  This gives each individual driver a chance to do
# cleanup.  This function will only be called once, after all the tests are
# run.  A driver need not implement it.  It is a virtual function.
# This method invoked unconditionally, even if no test in the group was executed. 
# See #globalCleanupConditional() method description.
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
# Sub: globalCleanupConditional
# Clean up after all tests have run, before #globalCleanup(). Invoked iff #globalSetupConditional()
# was previously invoked gor this config file (sequential mode) or test group (parallel mode).
# It is a virtual function. Subclasses may override it.
#
# Paramaters:
# globalHash - Top level hash from config file (does not have any group
# or test information in it).
# log - log file handle
#
# Returns:
# None
sub globalCleanupConditional() {
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
		$confFile, $startat, $logname, $resources ) = @_;

    my $subName = (caller(0))[3];
    my $msg="";
	# Rather than make each driver handle our multi-level cfg, we'll flatten
	# the hashes into one for it.
	my %globalHash;

	my $runAll = ((scalar(@$testsToRun) == 0) && (scalar(@$testsToMatch) == 0));

	# Read the global keys
	foreach (keys(%$cfg)) {
		next if $_ eq 'groups';
		$globalHash{$_} = $cfg->{$_};
	}

    my $report=0;
    my $properties= new Properties(0, $globalHash{'propertiesFile'});

    my $fileForkFactor = int($ENV{'FORK_FACTOR_FILE'});
    my $groupForkFactor = int($ENV{'FORK_FACTOR_GROUP'});
    # NB: this is to distinguish the sequential mode from parallel one: 
    my $productForkFactor = $fileForkFactor * $groupForkFactor;
    my $pm;
    if ($groupForkFactor > 1) {
        print $log "Group fork factor: $groupForkFactor\n";
        # Create the fork manager:
        $pm = new Parallel::ForkManager($groupForkFactor);
        # this is a callback method that will run in the main process on each job subprocess completion:
        $pm -> run_on_finish (
           sub {
              my ($pid, $exit_code, $identification, $exit_signal, $core_dump, $data_structure_reference) = @_; 
              # see what the child sent us, if anything
              if (defined($data_structure_reference)) { 
                  dbg("Group subprocess [$identification] finished, pid=${pid}, sent back: [$data_structure_reference].\n");
                  dbgDumpHash($data_structure_reference, "The hash passed in in the run_on_finish callback:");
                  putAll($testStatuses, $data_structure_reference);
                  dbgDumpHash($testStatuses, "The statuses after merge in the run_on_finish callback:");
              } else {
                  print "ERROR: Group subprocess [$identification] did not send back anything. Exit code = $exit_code\n";
              }
              my $subLogAgain = "$logname-$identification";
              appendFile($subLogAgain,$logname);
           }
       );
    } else {
    	# Do the global setup:
    	$self->globalSetup(\%globalHash, $log);
    }

    my $localStartAt = $startat;
	foreach my $group (@{$cfg->{'groups'}}) {
        my $groupName = $group->{'name'};

        my $subLog;
        my $subLogName;
        if ($groupForkFactor > 1) {
            # use group name as the Job id:
            my $jobId = $groupName;

            $subLogName = "$logname-$jobId";
            open $subLog, ">$subLogName" or die "FATAL ERROR $0 at ".__LINE__." : Can't open $subLogName, $!\n";

            dbg("**** Logging to [$subLogName].\n");
            # PARALLEL SECTION START: ===============================================================================
            $pm->start($jobId) and next;
            dbg("Started test group job \"$jobId\"\n");

            dbg("Doing setup for test group [$groupName]...\n");
            # Set the group-specific ID:
            # NB: note that '$globalHash' here is an object cloned for this subprocess.
            # So, there is no concurrency issue in using '$globalHash' there:
            $globalHash{'job-id'} = $globalHash{'job-id'} . "-" . $jobId;
            # Do the global setup which is specific for *this group*:
        	$self->globalSetup(\%globalHash, $subLog);
        } else {
            $subLog = $log;
            $subLogName = $logname;
        }

        # Run the group of tests.
        # NB: the processing of $localStartAt parameter happens only if the groupForkFactor < 1.
        my $sawStart = $self -> runTestGroup($groupName, $subLog, $confFile, \%globalHash, $group, $runAll, $testsToRun, $testsToMatch, $localStartAt, $testStatuses, $productForkFactor, $resources);
        if ((defined $localStartAt) && $sawStart) {
            undef $localStartAt;
        }

        if ($groupForkFactor > 1) {
            # do the clanups that are specific for *this group*.
            dbg("Doing cleanup for test group [$groupName]...\n");
            # NB: invoke it in such way to emphasize the fact that this method is not virtual:
            globalCleanupConditionalIf($self, \%globalHash, $subLog);
            $self->globalCleanup(\%globalHash, $subLog);

            dbg("Finishing test group [$groupName].\n");
            dbgDumpHash($testStatuses, "The satatuses hash at the fork section end");
            # NB: send the "testStatuses" hash object reference (which is local to this subprocess) to the parent process: 
            $subLog -> close();

            # TODO: may also consider the #runTestGroup block exit status and use it there.
            $pm -> finish(0, $testStatuses);
            # PARALLEL SECTION END. ===============================================================================
        }
	} # foreach $group  

    if ($groupForkFactor > 1) {
        $pm->wait_all_children;
    } else {
    	# Do the global cleanups:
        # NB: invoke it in such way to emphasize the fact that this method is not virtual:
        globalCleanupConditionalIf($self, \%globalHash, $log);
    	$self->globalCleanup(\%globalHash, $log);
    }
}

# Servce method to conditionally perform the virtual #globalCleanupConditional().
# NB: This sub should be "final" in Java terms,
# subclasses should not override it.
sub globalCleanupConditionalIf() {
    my ($self, $globalHash, $log) = @_;
    if (defined($globalHash->{$keyGlobalSetupConditionalDone})) {
        $self -> globalCleanupConditional($globalHash, $log);
    }
}

################################################################################
# Separated sub to run a test group.
# Parameters: (same named values from #run(...) sub with the same meaning).
# Returns: 'true' if the test defined by '$startat' was found, and 'false' otherwise.
#   (If the '$startat' is null, always returns true.)   
sub runTestGroup() {
        my ($self, $groupName, $subLog, $confFile, $globalHash, $group, $runAll, $testsToRun, $testsToMatch, $startat, $testStatuses, $productForkFactor, $resources) = @_;

        my $subName = (caller(0))[3];
        print $subLog "INFO $subName at ".__LINE__.": Running TEST GROUP(".$groupName.")\n";
        my $sawstart = !(defined $startat);
                
		my %groupHash = %$globalHash; 
		$groupHash{'group'} = $groupName;

		# Read the group keys
		foreach (keys(%$group)) {
			next if $_ eq 'tests';
			$groupHash{$_} = $group->{$_};
		}

        my $groupDuration=0;
        my $duration=0;

		# Run each test in the group:
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
            my $tmpTestHash = \%testHash;

			foreach (keys(%$test)) {
				$testHash{$_} = $test->{$_};
			}

			my $testName = $testHash{'group'} . "_" . $testHash{'num'};
            dbg("################### Executing test [$testName]...\n");

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
				print $subLog "Ignoring test $testName, ignore message: " .
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
					print $subLog "Skipping test $testName, it depended on " .
						"$testHash{$_} which returned a status of " .
						"$testStatuses->{$testHash{$_}}\n";
					$testStatuses->{$testName} = $dependStr;
					$skipThisOne = 1;
					last;
				}
			}
			if ($skipThisOne) {
                            if ($productForkFactor > 1) {
				printResults($testStatuses, $subLog, "Results so far", basename($confFile), $groupName);
                            } else {
				printResults($testStatuses, $subLog, "Results so far");
                            }
			    next;
			}

			print $subLog "\n******************************************************\n";
			print $subLog "\nTEST: $confFile::$testName\n";
			print $subLog  "******************************************************\n";
			print $subLog "Beginning test $testName at " . time . "\n";
            
            # At this point we're going to run the test for sure. 
            # So, do the preparation for that, if not yet done: 
            if (!defined($globalHash->{$keyGlobalSetupConditionalDone})) {
                $self -> globalSetupConditional($globalHash, $subLog);
                # this preparation should be done only *once* per each $globalHash instance,
                # so, set special flag to prevent #globalSetupConditional from being executed again:
                $globalHash->{$keyGlobalSetupConditionalDone} = 'true';
            }

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
				$testResult = $self->runTest(\%testHash, $subLog, $resources);
				$endTime = time;
				$benchmarkResult = $self->generateBenchmark(\%testHash, $subLog);
				my $result =
					$self->compare($testResult, $benchmarkResult, $subLog, \%testHash, $resources);
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
				print $subLog $msg;
				$duration = $endTime - $beginTime;
				$dbinfo{'duration'} = $duration;
				$self->recordResults($result, $testResult
                                          , $benchmarkResult, \%dbinfo, $subLog);
			};

			if ($@) {
				$msg= "ERROR $subName at : ".__LINE__." Failed to run test $testName <$@>\n";
				#print $msg;
				print $subLog $msg;
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
				$benchmarkResult, $subLog);
            #$report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName}, $testResult ) if ( $report );
            $report->testcase( $group->{'name'}, $testName, $duration, $msg, $testStatuses->{$testName} ) if ( $report );
            $groupDuration = $groupDuration + $duration;
                        if ($productForkFactor > 1) {
			   printResults( $testStatuses, $subLog, "Results so far", basename($confFile), $groupName );
                        } else {
			   printResults( $testStatuses, $subLog, "Results so far" );
                        }
		}  # for each test

        if ( $report ) {
            $report->systemOut( $subLogName, $group->{'name'});
            printGroupResultsXml( $report, $group->{'name'}, $testStatuses, $groupDuration );
        }
        $report = 0;
        return $sawstart;
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
