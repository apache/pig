#!/usr/bin/env perl

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
                                                                                       
use Sys::Hostname;

###############################################################################
# Class:  TestReport
# Used to record the results of a test run using junit xml schema
#

use strict;

package TestReport;
unshift( @INC, ".");

#@PigDbBootstrap::ISA=qw(Exporter);
require Exporter;
our @ISA = "Exporter";
#our @EXPORT_OK = qw( createDB createTablesDB removeDB changeConfigFileDB createDirectoriesHDFS copyTablesToHDFS);


use File::Path;
use IO::Handle;

#
# Set Path for perl Libraries
#

my $ROOT=undef;
if (defined $ENV{'HARNESS_ROOT'} ){
   $ROOT= $ENV{'HARNESS_ROOT'};

} else {
  die "FATAL ERROR: $0 - You must set HARNESS_ROOT to the root directory of the harness";
}

unshift( @INC, "$ROOT/libexec" );
unshift( @INC, ".");

require Properties;
require Log;

use strict;
use English;

my ($stdout, $stderr);
my %progress;

###############################################################################
# Sub: new
# Constructor for TestDriverPig is a subclass of TestDriver.
#
# Parameters:
# Properties  - Properties object with the global properties 
# FileName    - The name of the xml file.
#
# Returns:
# None
#


sub new() {
   my $class= shift;

   my %schema=('JUNIT',1 );
   my $self= {
       Class          => $class
      ,Properties     => undef
      ,FileName       => undef
      ,TmpFileName    => undef
      ,Schema         => undef
      ,Log            => undef
      ,TestcaseCount  => 0
   };

   bless( $self, $class );

   my $subName      = (caller(0))[3];
   my $usage= "USAGE: $class.new( Properties, FileName,Schema )";

   #################
   # Properties

   my $properties= shift;

   die "FATAL ERROR $subName at ".__LINE__." : Missing arguments to constructor!\n$usage\n"  if ( ! $properties );

   die "FATAL ERROR $subName at ".__LINE__." : Expected Properties as first argument, but got ".ref($properties)."\n$usage\n"  if ( ref( $properties ) !~ "Properties" );

   $self->{Properties}= $properties;

   #################
   # Set Log Object

   $self->{Log}= new Log ( $properties );
   my $log= $self->{Log};
   die "FATAL ERROR $0 $class.new() at ".__LINE__." : This is not a log objectt ".ref($log)."\n$usage\n"  if ( ref( $log ) !~ "Log" );

#   $log->msg( "DEBUG", $subName , __LINE__ , "Verify logging");

   #################
   # Set Filename for rESULTS


    $self->{FileName}= shift;
    $log->msg( "FATAL", $subName , __LINE__ , "Expected Filename as an argument. $usage\n" ) if ( ! $self->{FileName} );
   
   $self->{TmpFileName}= $self->{FileName} . '.tmp';

   return $self;

}

########################################################################
# Sub: totals
# Prints the totals for the group. It expects the individual tests
# elements to already have been printed. Next it will insert the header
# at the top of the file and the trailer at the end of the file.
# 
# Parameters:
# $group - the group for which results are to be printed
# $totalTests - the total tests executed during this test run for this group
# $failureCount - the total test for this group that failed
# $errorrCount  - the total test for this group that got an error
# $time         - the the total amount of time for this group  that it took to run tests
#
# Returns:
# None
#

sub totals(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my ( $group, $totalTests, $failureCount,  $errorCount, $time )= @_;
  
   my $host = Sys::Hostname::hostname();
   my $totals=
       '<?xml version="1.0" encoding="UTF-8" ?>'
     . "\n<testsuite errors=\"$errorCount\" failures=\"$failureCount\" hostname=\"$host\" name=\"$group\" tests=\"$totalTests\" time=\"$time\">" 
     . "\n<properties>" ;

   $self->insertMsg( '.', $totals );
   my $log         = $self->{Log};
   my $outfileName = $self->{FileName};

   open( OUT, ">>$outfileName" ) 
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $outfileName for writing\n");

   print OUT "\n</testsuite>";

   close( OUT);
}

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
sub testcase(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my ( $class, $test, $time, $status, $statusMsg, $result ) = @_;
   my $result;
  
   #If this is the first testcase element then print the properties trailer
   if ( $self->{TestcaseCount}== 0 ){
      $result= "\n</properties>";
      $self->printMsg( $result );
   }

   $self->{TestcaseCount}++;
   my $result = "";

   if ( $status =~ "SUCCEEDED" ){
      $result = "\n<testcase classname=\"$class\" name=\"$test\" time=\"$time\" />";

   } else {
#             . "\n<error type=\"$statusMsg\">i$result</error>"
      $result = "\n<testcase classname=\"$class\" name=\"$test\" time=\"$time\">" 
              . "\n<error type=\"$statusMsg\"></error>"
              . "\n</testcase>"
              ;
   }


  $self->printMsg( $result );
 
}

########################################################################
# Sub: systemOut
# Reads the log, finds where the test started and ended, prints that
# section of the log in the xml results under the  system out section of the schema.
# 
# Parameters:
# $logname    - the name of the test log containg stdout/stderr messages
# $groupiName - the group to which this test belongs
#
# Returns:
# None
#
sub systemOut(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my ( $logname, $groupName ) = @_;
   
   my $log      = $self->{Log};
   my $startTag    = "\n<system-out><![CDATA[";
   my $endTag      = "]]></system-out>";

   my $startPattern="Beginning test $groupName";
   my $endPattern  ="Beginning test";
   if  ( ! -e $logname ){ 
     
       $log->msg( "ERROR", $subName , __LINE__ , " $logname does not exist\n" );
       return;
   }


   open( IN, "<$logname" ) 
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $logname for reading\n" );

   $self->printMsg( $startTag );
   my $start=0;
   my $end  =0;
   while ( <IN> ) {

      $end++ if ( $_ !~ $startPattern && $_ =~ $endPattern && $start );
      $start++  if ( $_ =~ $startPattern );
      $_ =~ s/[\x00-\x08\x0B-\x0C\x0E-\x1F]//g;
      $self->printMsg( $_ ) if ( $start );
      last if ( $end );
   }

   $self->printMsg( $endTag );
   close ( IN );
 
}




########################################################################
# Sub: property
# Prints a single property in the xml file.
# 
# Parameters:
# $name    - property name
# $value   - property value
#
# Returns:
# None
#
sub property(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my ( $name, $value ) = @_;
   my $result = "\n<property name=\"$name\" value=\"$value\" />";
   $self->{PropertyCount}= $self->{PropertyCount}++;
   $self->printMsg( $result );
 
}

########################################################################
# Sub: insertMsg
# Insert a line into the report.
#
# Looks for particular pattern in the test report file, if it finds it
# then it inserts the message and prints the remainer of the file.
# 
# Parameters:
# $patern  - pttern to look for
# $msg     - message to insert
#
# Returns:
# None
sub insertMsg(){

   my $self = shift;
   my ( $pattern , $msg ) = @_;
   my $subName      = (caller(0))[3];
  
  
   my $infileName = $self->{FileName};
   my $outfileName = $self->{TmpFileName};
   my $log      = $self->{Log};
   
   open( IN, "<$infileName" ) 
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $infileName for writing\n");
   open( OUT, ">$outfileName" )  
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $outfileName for writing\n");

   my $isFirstOccurrenceOfPattern=0;
   my $lineCount=0;
   while ( <IN> ) {

      # removing blank lines at the top of the file - were causing post to fail
      # 10 is just a n arbritary number
      next if ( $_ =~ /^\s*$/ && $lineCount<10);
      $lineCount++;
      if ( $_ =~ $pattern && $isFirstOccurrenceOfPattern == 0 ) {

          $isFirstOccurrenceOfPattern++;
          print OUT $msg;

      }

      print OUT $_;
      
   }
   
   close( IN );
   close( OUT );

   $log->msg( "FATAL", $subName , __LINE__ , "File <$outfileName> does not exist\n") if ( ! -e $outfileName );
   my $cmd = "mv -f $outfileName $infileName";
   $log->msg( "DEBUG", $subName , __LINE__ , "$cmd");
   my @result= `$cmd`;
   if ( @result ) {
     $log->msg( "DEBUG", $subName , __LINE__ , "@result");
   }

 
}

########################################################################
# Sub: insertMsg
# Prints a message to  the report.
# 
# Parameters:
# $msg     - message to insert
#
# Returns:
#None
sub printMsg(){

   my $self         = shift;
   my $msg          = shift;
   my $subName      = (caller(0))[3];
   my $fileName     = $self->{FileName};
   my $log          = $self->{Log};
   
   open( OUT, ">>$fileName" ) 
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $fileName for writing\n");

   printf  OUT $msg;

   close( OUT );
  
}

########################################################################
# Sub: purge
# Purges the report, erasing all content.
# 
# Parameters:
# None
#
# Returns:
# None
sub purge(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my $log          = $self->{Log};
   my $fileName = $self->{FileName};

   open( OUT, ">$fileName" ) 
      || $log->msg( "FATAL", $subName , __LINE__ , "Could not open $fileName for writing\n");
  
   close ( OUT );
}

1;
