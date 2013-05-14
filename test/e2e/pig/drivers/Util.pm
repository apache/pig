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
# Class: Util
#
# A collection of  helper subroutines.
#


package Util;

##############################################################################
#  Sub: localTime
# 
#  Returns:
#  A string with the local time

sub  localTime() {

   my $retval = time();
   
   my $local_time = gmtime( $retval);

   return $local_time;

}

##############################################################################
#  Sub: formatedTime
#  Returns the time with following format "$mday/$mon/$year $hour:$min:$sec $weekday[$wday]"
#
#  Returns:
#  formated time

sub  formatedTime() {

   my @weekday = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat");

   my $retval = time();
   
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = gmtime(time);
   $year = $year + 1900; 
   $mon += 1;
   return  "$mday/$mon/$year $hour:$min:$sec $weekday[$wday]\n";
   

}

##############################################################################
# Sub: execCmd
# Records the command in the log and prints it to stdout before executing.
#
# Paramaters:
# $log     - The log object 
# $subName - The name of the subroutine the message originated at
# $lineNo  - The line number  of the subroutine the message originated at
# $cmd     - The command string to execute
# $msg     - A string, the message to print
# $level   - (optional)The logging level for the message: DEBUG, INFO, WARN, ERROR, FATAL
#            defaults to DEBUG.
#
#  Returns:
#  An array containing the  output from the executed command.
#

sub execCmd() {

    my ( $log, $subName, $lineNo, $cmd, $level ) = @_ ;

    my $count = @_;
    my $thisSubName = (caller(0))[3];

   #Check for errors in arguments
   if ( $count < 4 ){

       if ( $log ) {
          $log->msg( $level, $thisSubName, __LINE__ , "Invalid number of arguments, got $count=( @_ )" );

       } else {
          print "ERROR: $0 $thisSubName at ".__LINE__."Invalid number of arguments\n";
    
       }
       return 1;
   }

   #Log command,  execute commdand, return results
   $level  = "DEBUG" if ( !$level );
   $log->msg( $level, $subName, $lineNo , "$cmd");

    my @result = `$cmd`;

    $log->msg( $level, $subName, $lineNo , "@cmd") if ( @cmd );

    return @result;
}

sub getHadoopCmd
{
    my ( $properties ) = @_;

    my $subName        = (caller(0))[3];
    my @baseCmd;

    die "$0.$subName: null properties" if (! $properties );

    my $cmd;

    $cmd = $properties->{'gridstack.root'} . "/hadoop/current/bin/hadoop";
    if ( ! -x "$cmd" ) {
      print STDERR "\n$0::$subName WARNING: Can't find hadoop command: $cmd\n";
      $cmd = `which hadoop`;
      chomp $cmd;
      print STDERR "$0::$subName WARNING: Instead using command: $cmd\n";
    }
    if ( ! -x "$cmd" ) {
      die "\n$0::$subName FATAL: Hadoop command does not exist: $cmd\n";
    }
    push (@baseCmd, $cmd);

    push (@baseCmd, '--config', $properties->{'testconfigpath'}) if defined($properties->{'testconfigpath'});

    return @baseCmd;
}


sub getHiveCmd
{
    my ( $properties ) = @_;

    my $subName        = (caller(0))[3];
    my @baseCmd;

    die "$0.$subName: null properties" if (! $properties );

    my $cmd;

    $cmd = $properties->{'hive_bin_location'} . "/hive";
    if ( ! -x "$cmd" ) {
      die "\n$0::$subName FATAL: Hive command does not exist: $cmd\n";
    }
    push (@baseCmd, $cmd);

#   push (@baseCmd, '--config', $properties->{'testconfigpath'}) if defined($properties->{'testconfigpath'});

    return @baseCmd;
}

sub getHowlCmd
{
    my ( $properties ) = @_;

    my $subName        = (caller(0))[3];
    my @baseCmd;

    die "$0.$subName: null properties" if (! $properties );

    my $cmd;

    $cmd = $properties->{'howl_bin_location'} . "/howl";
    if ( ! -x "$cmd" ) {
      die "\n$0::$subName FATAL: Howl command does not exist: $cmd\n";
    }
    push (@baseCmd, $cmd);

    return @baseCmd;
}



sub getPigCmd
{
    my $subName        = (caller(0))[3];
    my $jarkey         = shift;
    my ( $properties ) = @_;
    my $isPigSqlEnabled= 0;
    my @baseCmd;
    die "$0.$subName: null properties" if (! $properties );

    #UGLY HACK for pig sql support
    if ( $jarkey =~ /testsql/ ) {

       $isPigSqlEnabled= 1;
       $jarkey = "testjar";

    }

    my $cmd;
    if ( $properties->{'use-pig.pl'} ) {
      # The directive gives that
      # 1) the 'pig' command will be called, as opposed to java
      # 2) the conf file has full control over what options and parameters are 
      #    passed to pig. 
      #    I.e. no parameters should be passed automatically by the script here. 
      #
      # This allows for testing of the pig script as installed, and for testin of
      # the pig script's options, including error testing. 

      $pigLoc = "/bin/pig";
      if ($properties->{'usePython'} eq "true") {
        # print "Using python\n";
        $pigLoc = "/bin/pig.py";
        push(@baseCmd, "python");
      }
      $cmd = $properties->{'gridstack.root'} . "/pig/" . $properties->{'pigTestBuildName'} . $pigLoc;

      if ( ! -x "$cmd" ) {
        print STDERR "\n$0::$subName WARNING: Can't find pig command: $cmd\n";
        $cmd = `which pig`;
        chomp $cmd;
        print STDERR "$0::$subName WARNING: Instead using command: $cmd\n";
      }
      die "\n$0::$subName FATAL: Pig command does not exist: $cmd\n" if ( ! -x $cmd  );
      push (@baseCmd, $cmd );

      if ( $properties->{'use-pig.pl'} eq 'raw' ) { # add _no_ arguments automatically
        # !!! 
	return @baseCmd;
      }

    } else {
        $cmd="java";

        # Set JAVA options

        # User can provide only one of
        # (-c <cluster>) OR (-testjar <jar> -testconfigpath <path>)
        # "-c <cluster>" is allowed only in non local mode
        if(defined($properties->{'cluster.name'})) {
            # use provided cluster
            @baseCmd = ($cmd, '-c', $properties->{'cluster.name'});
        } else {
    
                die "\n$0::$subName FATAL: The jar file name must be passed in at the command line or defined in the configuration file\n" if ( !defined( $properties->{$jarkey} ) );
                die "\n$0::$subName FATAL: The jar file does not exist.\n" . $properties->{$jarkey}."\n" if ( ! -e  $properties->{$jarkey}  );
    
            # use user provided jar
                my $classpath;

				if (defined $properties->{'jythonjar'}) {
					$classpath = "$classpath:" . $properties->{'jythonjar'};
				}
				if (defined $properties->{'jrubyjar'}) {
					$classpath = "$classpath:" . $properties->{'jrubyjar'};
				}
                if( $properties->{'exectype'} eq "local") {
                   # in local mode, we should not use
                   # any hadoop-site.xml
                   $classpath= "$classpath:" . $properties->{$jarkey};
                   $classpath= "$classpath:$properties->{'classpath'}" if ( defined( $properties->{'classpath'} ) );
                   @baseCmd = ($cmd, '-cp', $classpath, '-Xmx1024m');
    
                } else {
    
                   # non local mode, we also need to specify
                   # location of hadoop-site.xml
                   die "\n$0::$subName FATAL: The hadoop configuration file name must be passed in at the command line or defined in the configuration file\n" 
			if ( !defined( $properties->{'testconfigpath'} ) );
                   die "\n$0::$subName FATAL $! " . $properties->{'testconfigpath'}."\n\n"  
                   	if (! -e $properties->{'testconfigpath'} );

                   $classpath= "$classpath:" . $properties->{$jarkey}.":".$properties->{'testconfigpath'};
                   $classpath= "$classpath:$properties->{'classpath'}" if ( defined( $properties->{'classpath'} ) );
                   $classpath= "$classpath:$properties->{'howl.jar'}" if ( defined( $properties->{'howl.jar'} ) );
                   @baseCmd = ($cmd, '-cp', $classpath );
            }
        }
    
        # sets the queue, for exampel "grideng"
        if(defined($properties->{'queue'})) {
          push( @baseCmd,'-Dmapred.job.queue.name='.$properties->{'queue'});
        }
    
        if(defined($properties->{'additionaljars'})) {
          push( @baseCmd,'-Dpig.additional.jars='.$properties->{'additionaljars'});
        }
    
        if( ( $isPigSqlEnabled == 1 ) ){

	    if(defined($properties->{'metadata.uri'})) {
		push( @baseCmd, '-Dmetadata.uri='.$properties->{'metadata.uri'});
	    }

	    if(defined($properties->{'metadata.impl'})) {
		push( @baseCmd, '-Dmetadata.impl='.$properties->{'metadata.impl'});
	    }else{
		push( @baseCmd, '-Dmetadata.impl=org.apache.hadoop.owl.pig.metainterface.OwlPigMetaTables');
	    }
        }

        # Add howl support
	if(defined($properties->{'howl.metastore.uri'})) {
	  push( @baseCmd, '-Dhowl.metastore.uri='.$properties->{'howl.metastore.uri'});
	}
    
      # Set local mode property
      # if ( defined($properties->{'exectype'}) && $properties->{'exectype'}=~ "local" ) {
      # Removed above 'if...' for Pig 8.
        my $java=`which java`;
        my $version=`file $java`;
        if ( $version =~ '32-bit' ){
           push(@baseCmd,'-Djava.library.path='.$ENV{HADOOP_HOME}.'/lib/native/Linux-i386-32');
        } else {
           push(@baseCmd,'-Djava.library.path='.$ENV{HADOOP_HOME}.'/lib/native/Linux-amd64-64');
        }
      # }


        # Add user provided java options if they exist
        if (defined($properties->{'java_params'})) {
          push(@baseCmd, @{$properties->{'java_params'}});
        }
    
        if(defined($properties->{'hod'})) {
          push( @baseCmd, '-Dhod.server=');
        }

      # sets the permissions on the jobtracker for the logs
      push( @baseCmd,'-Dmapreduce.job.acl-view-job=*');


      # Add Main
      push(@baseCmd, 'org.apache.pig.Main');

      # Set local mode PIG option
      if ( defined($properties->{'exectype'}) && $properties->{'exectype'}=~ "local" ) {
          push(@baseCmd, '-x');
          push(@baseCmd, 'local');
      }

      # Set Pig SQL options
      if( ( $isPigSqlEnabled == 1 ) && defined($properties->{'metadata.uri'})) {
  
         if ( defined($properties->{'testoutpath'}) ) {
           push( @baseCmd, '-u' );
           push( @baseCmd, $properties->{'testoutpath'} );
         }
  
         push( @baseCmd, '-s' );
         push( @baseCmd, '-f' );
      }

    } # end else of if use-pig.pl


    # Add -latest or -useversion 
    if ( $cmd =~ 'pig$' ) {
      # Add -latest, or -useversion if 'current' is not target build
      if ( defined($properties->{'pigTestBuildName'})) {
        if ($properties->{'pigTestBuildName'} eq 'latest') {
            push(@baseCmd, '-latest');
        } elsif ($properties->{'pigTestBuildName'} ne 'current') {
            push(@baseCmd, '-useversion', "$properties->{'pigTestBuildName'}");
        }
      }
    } elsif ( $cmd =~ 'java' ) {

      # is this ever used: ???
      # Add latest if it's there
      if (defined($properties->{'latest'})) {
          push(@baseCmd, '-latest');
      }
    }

    return @baseCmd;
}


sub getBasePigSqlCmd 
{

    my $subName        = (caller(0))[3];
   
   Util::getPigCmd( 'testsql', @_ );

}

sub getBasePigCmd 
{

    my $subName        = (caller(0))[3];
   
   Util::getPigCmd( 'testjar', @_ );

}

sub getLatestBasePigCmd 
{

    my $subName        = (caller(0))[3];
   
   Util::getPigCmd( 'latesttestjar', @_ );

}


sub getBenchmarkBasePigCmd 
{
    my $subName        = (caller(0))[3];
    my ( $properties ) = @_;

   Util::getPigCmd( 'benchmarkjar', @_ );

}

sub setLocale
{
   my $locale= shift;
#   $locale = "en_US.UTF-8" if ( !$locale );
$locale = "ja_JP.utf8" if ( !$locale );
   $ENV[LC_CTYPE]="$locale";
   $ENV[LC_NUMERIC]="$locale";
   $ENV[LC_TIME]="$locale";
   $ENV[LC_COLLATE]="$locale";
   $ENV[LC_MONETARY]="$locale";
   $ENV[LC_MESSAGES]="$locale";
   $ENV[LC_PAPER]="$locale";
   $ENV[LC_NAME]="$locale";
   $ENV[LC_ADDRESS]="$locale";
   $ENV[LC_TELEPHONE]="$locale";
   $ENV[LC_MEASUREMENT]="$locale";
   $ENV[LC_IDENTIFICATION]="$locale";
}

sub getLocaleCmd 
{
  my $locale= shift;
  $locale = "en_US.UTF-8" if ( !$locale );

  return     "export LC_CTYPE=\"$locale\";"
          ."export LC_NUMERIC=\"$locale\";"
          ."export LC_TIME=\"$locale\";"
          ."export LC_COLLATE=\"$locale\";"
          ."export LC_MONETARY=\"$locale\";"
          ."export LC_MESSAGES=\"$locale\";"
          ."export LC_PAPER=\"$locale\";"
          ."export LC_NAME=\"$locale\";"
          ."export LC_ADDRESS=\"$locale\";"
          ."export LC_TELEPHONE=\"$locale\";"
          ."export LC_MEASUREMENT=\"$locale\";"
          ."export LC_IDENTIFICATION=\"$locale\"";
}

sub isWindows
{
    if($^O =~ /mswin/i) {
        return 1;
    }
    else {
        return 0;
    }
}
1;
