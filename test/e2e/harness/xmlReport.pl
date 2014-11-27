#!/usr/bin/env perl
use Sys::Hostname;

use strict;

use File::Path;
use IO::Handle;

#
# Set Path for perl Libraries
#
unshift( @INC, ".");


########################################################################
# Sub: testcase
# Prints the result for a test case.
# 
# Parameters:
# $class - the test file
# $test  - the test name
# $time  - the the total amount of time it took to run a test
#
# Returns:
# None
#
sub getTestcase($$$$$$){

   my ( $class_name, $group_name, $test_name, $time, $status, $msg )=@_;
   my $result = "";

   if ( $status =~ "passed" ){
      $result = "\n<testcase classname=\"$class_name\" name=\"$test_name\" time=\"$time\"/>";

   } elsif ( $status =~ "skipped" ){
      $result = "\n<testcase classname=\"$class_name\" name=\"$test_name\" time=\"0\">"
              . "\n<skipped type=\"$status\">$msg</skipped>"
              . "\n</testcase>"
              ;
   } else {
      $result = "\n<testcase classname=\"$class_name\" name=\"$test_name\" time=\"$time\">" 
              . "\n<error type=\"$status\">$msg</error>"
              . "\n</testcase>"
              ;
   } return $result; 
}


sub printXmlReport($) {

   my $host = Sys::Hostname::hostname();
   my $tmpFileName =  shift;
   my $reportFileName =  shift;

   my $testcases = "";
   my $total_time= 0;
   my $testnameSuffix = "";

   if ($tmpFileName =~ m/-local/) {
      $testnameSuffix = "_local";
   }

   my $passedCount  = 0;
   my $failureCount = 0;
   my $errorCount   = 0;
   my $skippedCount = 0;
   my $totalCount = 0;

   my %test2starttime = { '', 0 };

   open( IN, "$tmpFileName")
      || die "Could not open $tmpFileName\n";

   while(<IN>){

      my $line = $_;
      # e.g.: "Beginning test Checkin_1 at 1346855793"
      if ( $line =~ m/Beginning test (.*) at (.*)/ ) {
	# put "test - start time" pair to the hash:	
        $test2starttime{ $1 } = $2; 
      } else { 
       if ( $line =~ "TestDriver::run" ) {
	 my $duration = 0;
         if ( $line =~ m/TestDriver::run.*Failed to run test (.*) <(.*)/ ) {
            # ERROR TestDriver::run at : 470 Failed to run test ClassResolution_1 <Failed running ./out/pigtest/hadoopqa/hadoopqa.1327755958/ClassResolution_1_benchmark.pig
            #print "test aborted: $line";
            $testcases .= getTestcase ( $1, "group", $1 . $testnameSuffix, 0, "aborted", $2 );   
            $errorCount++;
         } elsif ( $line =~ m/TestDriver::run.*Test (.*) SUCCEEDED at (.*)/ ) {
            # INFO: TestDriver::run() at 444:Test Unicode_cmdline_1 SUCCEEDED at 1327751873 
            #print "test passed: $1 $2 line: $line";
            $passedCount++;
            $duration = $2 - $test2starttime{ $1 }; 
            $testcases .= getTestcase ( $1, "group", $1 . $testnameSuffix, $duration, "passed", "" );     
         } elsif ( $line =~ m/TestDriver::run.*Test (.*) FAILED at (.*)/ ) {
            $failureCount++;
            $duration = $2 - $test2starttime{ $1 };
            $testcases .= getTestcase ( $1, "group", $1 . $testnameSuffix, $duration, "failed", "" );
            #print "test failed: $1 $2 line: $line";
         } elsif ($line =~ "Running TEST GROUP") {
            next;
         } elsif ($line =~ m/TestDriver::run.*Test (.*) SKIPPED at (.*)/) {
            # INFO: TestDriver::run() at 444:Test StreamingLocalErrors_1 SKIPPED at 1327923441
            $skippedCount++;
            $duration = $2 - $test2starttime{ $1 };
            $testcases .= getTestcase ( $1, "group", $1 . $testnameSuffix, $duration, "skipped", "" );
         } else {
            print STDERR "Ignored line: $line";
            next;
         }    
         #$total_time= $total_time + $time;
         $totalCount++;
      }
     }

   }
   close(IN);
   #Report
   my $host = Sys::Hostname::hostname();
   my $run_name = "e2e tests";
   my $report=
       '<?xml version="1.0" encoding="UTF-8" ?>'
     . "\n<testsuite errors=\"$errorCount\" failures=\"$failureCount\" skips=\"$skippedCount\"  hostname=\"$host\" name=\"$run_name\" tests=\"$totalCount\" time=\"$total_time\">" 
     . "\n$testcases" 
     . "\n</testsuite>" 
     . "\n";
   print $report;
}


if (!defined( $ARGV[0] )) {
   die "No input log file specified\n";
}

printXmlReport($ARGV[0]);



