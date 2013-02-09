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
                                                                                       
###########################################################################
# Class: Log
#
# Provides a simple logging framework. 
# - Supports logging levels: DEBUG, INFO, WARN, ERROR, FATAL
# - Supports console and file logging.  Console logging defaults to ERROR,
#   and file logging defaults to FATAL.
# - Console and file logging levels are configurable thru the properties file.
#

package Log;

use strict;

unshift( @INC, ".");

#@PigDbBootstrap::ISA=qw(Exporter);
require Exporter;
our @ISA = "Exporter";
#our @EXPORT_OK = qw( createDB createTablesDB removeDB changeConfigFileDB createDirectoriesHDFS copyTablesToHDFS);


use English;
use File::Path;
use IO::Handle;

#
# Set Path for perl Libraries
#

use Properties;


my ($stdout, $stderr);
my %progress;

##############################################################################
#  Sub: new
#  Log Constructor. The constructor sets the default logging levels as
#  follows:
#  - DEBUG for the logs
#  - ERROR for the console
# 
#
# Paramaters:
# Properties      - a Properties object containing the fraemwork properties
#                   various configuration properties for the log will be referenced
#                   in subsequent subroutines.
# Returns: 
# $self - object reference
#

# LogName         - the name of the log
# LogLevelNo      - the loging level for the Log
# ConsoleLevelNo  - the loging level for the console
# ValidLevelRef   - a hash used to look up valid logging levels

sub new() {
   my $class= shift;

   
   my %validLevel=('DEBUG',1,'INFO',2,'WARN',3,'ERROR',4,'FATAL',5);
   my $self= {
       Properties     => undef
      ,LogName        => undef
      ,LogLevelNo     => undef
      ,ConsoleLevelNo => undef
      ,ValidLevelRef  => \%validLevel
   };

   bless( $self, $class );
 
   my $usage=  "USAGE: $class.new( Properties )" 
             . "\nMust set the following properties:" 
             . "\tharness.log"
             . "\tharness.log.level"
             . "\tharness.console.level"
             ;

   #################
   # Properties

   my $properties= shift;
   die "FATAL ERROR $class.new() at ".__LINE__." : Missing arguments to constructor!\n$usage\n"  if ( ! $properties );
   die "FATAL ERROR $class.new() at ".__LINE__." : Expected Properties as first argument, but got ".ref($properties)."\n$usage\n"  if ( ref( $properties ) !~ "Properties" );
   $self->{Properties}= $properties;

   #################
   # Verify Log File

   my $logName = $self->{Properties}{'harness.log'};
   die "FATAL ERROR $class.new()  at ".__LINE__." : no log name specified in properties file\n$usage\n"  if ( !$logName );
   
   open( LOG, ">>$logName" ) 
      || die "FATAL ERROR $class.new() at " .__LINE__. " : Could not open <$logName> for writing\n";
  
   close ( LOG );
   $self->{LogName}= $logName;

   #################
   # Set Logging level

   my $logLevel= $self->{Properties}{'harness.log.level'};
   if ( ! $logLevel ){
     $logLevel = 'DEBUG';
   } else {
     $logLevel = 'DEBUG' if ( ! $validLevel{$logLevel} );
   }
   $self->{LogLevelNo}= $validLevel{ $logLevel };

   # Set Console level

   my $consoleLevel = $self->{Properties}{'harness.console.level'};

   if ( !$consoleLevel ){ 
     $consoleLevel = 'ERROR';

   } else {
     $consoleLevel = 'ERROR' if ( ! $validLevel{$consoleLevel} );
   }
   $self->{ConsoleLevelNo} = $validLevel{ $consoleLevel };

   return $self;
}

##############################################################################
#  Sub: msg
#  Log Constructor. The constructor sets the default logging levels as
#  follows:
#  - DEBUG for the logs
#  - ERROR for the console
# 
#
# Paramaters:
# level           - The logging level for the message: DEBUG, INFO, WARN, ERROR, FATAL
# subName         - The name of the subroutine the message originated at 
# lineNo          - The line number  of the subroutine the message originated at 
# msg             - A string, the message to print
#
sub msg(){

   my $self = shift;
   my ( $level, $subName, $lineNo, $msg )=@_;
   my $ref= $self->{ValidLevelRef};
   my %validLevel= %$ref;

   #IF level is empty then set it to ERROR
   $level = 'ERROR' if ( ! $level );

   #IF level is invalid then set it to ERROR
   my $levelNo=$validLevel{ $level };
   if ( $levelNo !=0 && !$levelNo){
     $level   = 'ERROR';
     $levelNo=$validLevel{ $level };
   }
   
   if ( $levelNo >=  $self->{LogLevelNo} ){
       $self->printToLog( $level, $subName, $lineNo, $msg);
   }
   if ( $levelNo >=  $self->{ConsoleLevelNo} ){
       $self->printToConsole( $level, $subName, $lineNo, $msg);
   }
   return $msg;
}

##############################################################################
#  Sub: printToConsole
#  A private subroutine that formats the string for the console.
#
# Paramaters:
# level           - The logging level for the message: DEBUG, INFO, WARN, ERROR, FATAL
# subName         - The name of the subroutine the message originated at 
# lineNo          - The line number  of the subroutine the message originated at 
# msg             - A string, the message to print
# 
sub printToConsole(){

   my $self = shift;
   my ( $level, $subName, $lineNo, $msg )= @_;

   my $ref= $self->{ValidLevelRef};
   my %validLevel= %$ref;

   if (  $self->{ConsoleLevelNo} ==  $validLevel{'DEBUG'} ){
       printf  '%7s %15s %5s : %s', $level, $subName, $lineNo, "$msg\n" ;
   } else {
      printf  '%7s : %s', $level, "$msg\n" ;

   }

}

##############################################################################
#  Sub: printToLog
#  A private subroutine that formats the string for the log.
#
# Paramaters:
# level           - The logging level for the message: DEBUG, INFO, WARN, ERROR, FATAL
# subName         - The name of the subroutine the message originated at 
# lineNo          - The line number  of the subroutine the message originated at 
# msg             - A string, the message to print
# 
sub printToLog(){

   my $self = shift;
   my $selfSubName      = (caller(0))[3];
   my ( $level, $subName, $lineNo, $msg )= @_;

   my $ref= $self->{ValidLevelRef};
   my %validLevel= %$ref;

   my $logName = $self->{LogName};

   open( LOG, ">>$logName" ) 
      || die "FATAL ERROR $selfSubName() at " .__LINE__.": Could not open <$logName> for writing\n";
   
   my $now = time();
   my $localTime = gmtime($now);
   printf  LOG '%25s %7s  %28s %5s     %s', $localTime, $level, $subName, $lineNo, "$msg\n" ;

   close( LOG );
  
}

##############################################################################
#  Sub: purge
#  A public subroutine that purges the log.
#
# Paramaters:
# logName         - The name of the log to purge
# 
sub purge(){

   my $self = shift;
   my $subName      = (caller(0))[3];
   my $logName = $self->{LogName};

   open( LOG, ">$logName" ) 
      || die "FATAL ERROR $subName() at " .__LINE__. ": Could not open $logName for writing\n";
  
   close ( LOG );
}

1;
