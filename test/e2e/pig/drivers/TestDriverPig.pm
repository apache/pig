package TestDriverPig;

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
                                                                                       
###############################################################################
# Test driver for pig nightly tests.
# 
#

use TestDriver;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method
use Digest::MD5 qw(md5_hex);
use Util;
use File::Path;

use strict;
use English;

our $className= "TestDriver";
our @ISA = "$className";
our $ROOT = (defined $ENV{'PIG_HARNESS_ROOT'} ? $ENV{'PIG_HARNESS_ROOT'} : die "ERROR: You must set environment variable PIG_HARNESS_ROOT\n");
our $toolpath = "$ROOT/libexec/PigTest";

my $passedStr  = 'passed';
my $failedStr  = 'failed';
my $abortedStr = 'aborted';
my $skippedStr = 'skipped';
my $dependStr  = 'failed_dependency';

sub new
{
    # Call our parent
    my ($proto) = @_;
    my $class = ref($proto) || $proto;
    my $self = $class->SUPER::new;

    $self->{'exectype'} = "mapred"; # till we know better (in globalSetup())!
    $self->{'ignore'} = "true";     # till we know better (in globalSetup())!

    bless($self, $class);
    return $self;
}

###############################################################################
# This method has been copied over from TestDriver to make changes to
# support skipping tests which do not match current execution mode
# or which were marked as 'ignore'
#
#
# Static function, can be used by test_harness.pl
# Print the results so far, given the testStatuses hash.
# @param testStatuses - reference to hash of test status results.
# @param log - reference to file handle to print results to.
# @param prefix - A title to prefix to the results
# @returns nothing.
#
sub printResults
{
    my ($testStatuses, $log, $prefix) = @_;

    my ($pass, $fail, $abort, $depend, $skipped) = (0, 0, 0, 0, 0);

    foreach (keys(%$testStatuses)) {
        ($testStatuses->{$_} eq $passedStr)  && $pass++;
        ($testStatuses->{$_} eq $failedStr)  && $fail++;
        ($testStatuses->{$_} eq $abortedStr) && $abort++;
        ($testStatuses->{$_} eq $dependStr)  && $depend++;
        ($testStatuses->{$_} eq $skippedStr) && $skipped++;
    }   

    my $msg = "$prefix, PASSED: $pass FAILED: $fail SKIPPED: $skipped ABORTED: $abort " .
        "FAILED DEPENDENCY: $depend";
    print $log "$msg\n";
	print "$msg\r";
}


sub replaceParameters
{
##!!! Move this to Util.pm

    my ($self, $cmd, $outfile, $testCmd, $log) = @_;

    # $self
    $cmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    # $outfile
    $cmd =~ s/:OUTPATH:/$outfile/g;

    # $ENV
    $cmd =~ s/:PIGHARNESS:/$ENV{PIG_HARNESS_ROOT}/g;

    # $testCmd
    $cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
    $cmd =~ s/:OUTPATH:/$outfile/g;
    $cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
    $cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
    $cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
    $cmd =~ s/:HADOOPHOME:/$testCmd->{'hadoopHome'}/g;
    $cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
    $cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
    $cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
    $cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
    $cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
    $cmd =~ s/:LOCALTESTPATH:/$testCmd->{'localpathbase'}/g;
    $cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
    $cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;

    if ( $testCmd->{'hadoopSecurity'} eq "secure" ) { 
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteSecureCluster'}/g;
    } else {
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteNotSecureCluster'}/g;
    }

    return $cmd;
}

sub globalSetup
{
    my ($self, $globalHash, $log) = @_;
    my $subName = (caller(0))[3];


    # Setup the output path
    my $me = `whoami`;
    chomp $me;
    $globalHash->{'runid'} = $me . "." . time;

    # if "-ignore false" was provided on the command line,
    # it means do run tests even when marked as 'ignore'
    if(defined($globalHash->{'ignore'}) && $globalHash->{'ignore'} eq 'false')
    {
        $self->{'ignore'} = 'false';
    }

    # if "-x local" was provided on the command line,
    # it implies pig should be run in "local" mode -so 
    # change input and output paths 
    if(defined($globalHash->{'x'}) && $globalHash->{'x'} eq 'local')
    {
        $self->{'exectype'} = "local";
        $globalHash->{'inpathbase'} = $globalHash->{'localinpathbase'};
        $globalHash->{'outpathbase'} = $globalHash->{'localoutpathbase'};
    }
    $globalHash->{'outpath'} = $globalHash->{'outpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {
        $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
    }
    else {
        $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }

    my @cmd = ($self->getPigCmd($globalHash, $log), '-e', 'mkdir', $globalHash->{'outpath'});

    if($self->{'exectype'} eq "local")
    {
        @cmd = ('mkdir', '-p', $globalHash->{'outpath'});
    }


    print $log join(" ", @cmd);

    if($self->{'exectype'} eq "mapred")
    {
       IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";
    }
    else
    {
       IPC::Run::run(\@cmd, \undef, $log, $log) or die "Cannot create directory " . $globalHash->{'outpath'} . "\n";
    }

    IPC::Run::run(['mkdir', '-p', $globalHash->{'localpath'}], \undef, $log, $log) or 
        die "Cannot create localpath directory " . $globalHash->{'localpath'} .
        " " . "$ERRNO\n";
}


sub runTest
{
    my ($self, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'pig'} && $self->hasCommandLineVerifications( $testCmd, $log) ) {
       return $self->runPigCmdLine( $testCmd, $log, 1);
    } elsif( $testCmd->{'pig'} ){
       return $self->runPig( $testCmd, $log, 1);
   #} elsif(  $testCmd->{'pigsql'} ){
   #   return $self->runPigSql( $testCmd, $log, $copyResults );
    } elsif(  $testCmd->{'script'} ){
       return $self->runScript( $testCmd, $log );
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}


sub runPigCmdLine
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outdir  = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stdoutfile = "$outdir/stdout";
    my $stderrfile = "$outdir/stderr";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the pig script to a file.
    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);

    # Build the command
    my @baseCmd = $self->getPigCmd($testCmd, $log);
    my @cmd = @baseCmd;

    # Add option -l giving location for secondary logs
    ##!!! Should that even be here? 
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-logfile");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    # Add pig file and redirections 
    push(@cmd, $pigfile);
    my $command= join (" ", @cmd) . " 1> $stdoutfile 2> $stderrfile";

    # Run the command
    print $log "$0:$subName Going to run command: ($command)\n";
    print $log "$0:$subName STD OUT IS IN FILE ($stdoutfile)\n";
    print $log "$0:$subName STD ERROR IS IN FILE ($stderrfile)\n";
    print $log "$0:$subName PIG SCRIPT CONTAINS ($pigfile):  \n$pigcmd\n";

    my @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n$result{'stderr'}\n";

    return \%result;
}


sub runScript
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    # Set up file locations
    my $script = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".sh";
    my $outdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $outfile = "$outdir/script.out";
    my $stdoutfile = "$outdir/script.out";
    my $stderrfile = "$outdir/script.err";

    mkpath( [ $outdir ] , 0, 0755) if ( ! -e outdir );
    if ( ! -e $outdir ){
       print $log "$0.$subName FATAL could not mkdir $outdir\n";
       die "$0.$subName FATAL could not mkdir $outdir\n";
    }

    # Write the script to a file
    my $cmd = $self->replaceParameters( $testCmd->{'script'}, $outfile, $testCmd, $log );

    open(FH, ">$script") or die "Unable to open file $script to write script, $ERRNO\n";
    print FH $cmd . "\n";
    close(FH);

    my @result=`chmod +x $script`;

    # Build the command
    my $command= "$script 1> $stdoutfile 2> $stderrfile";

    # Run the script
    print $log "$0:$subName Going to run command: ($command)\n";
    print $log "$0:$subName STD OUT IS IN FILE ($stdoutfile)\n";
    print $log "$0:$subName STD ERROR IS IN FILE ($stderrfile)\n";
    print $log "$0:$subName SCRIPT CONTAINS ($script):  \n$cmd\n";

    @result=`$command`;
    $result{'rc'} = $? >> 8;
    $result{'output'} = $outfile;
    $result{'stdout'} = `cat $stdoutfile`;
    $result{'stderr'} = `cat $stderrfile`;
    $result{'stderr_file'} = $stderrfile;

    print $log "STD ERROR CONTAINS:\n$result{'stderr'}\n";

    return \%result;
}


sub getPigCmd($$$)
{
    my ($self, $testCmd, $log) = @_;

    my @pigCmd;

    # set the PIG_CLASSPATH environment variable
	my $pcp .= $testCmd->{'jythonjar'} if (defined($testCmd->{'jythonjar'}));
    $pcp .= ":" . $testCmd->{'classpath'} if (defined($testCmd->{'classpath'}));
    $pcp .= ":" . $testCmd->{'testconfigpath'} if ($testCmd->{'exectype'} ne "local");

    # Set it in our current environment.  It will get inherited by the IPC::Run
    # command.
    $ENV{'PIG_CLASSPATH'} = $pcp;

    @pigCmd = ("$testCmd->{'pigpath'}/bin/pig");

    if (defined($testCmd->{'additionaljars'})) {
        push(@pigCmd, '-Dpig.additional.jars='.$testCmd->{'additionaljars'});
    }

    if ($testCmd->{'exectype'} eq "local") {
		push(@{$testCmd->{'java_params'}}, "-Xmx1024m");
        push(@pigCmd, ("-x", "local"));
    }

    if (defined($testCmd->{'java_params'})) {
		$ENV{'PIG_OPTS'} = join(" ", @{$testCmd->{'java_params'}});
    }


    return @pigCmd;
}



sub runPig
{
    my ($self, $testCmd, $log, $copyResults) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, "> $pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);


    # Build the command
    my @baseCmd = $self->getPigCmd($testCmd, $log);
    my @cmd = @baseCmd;

    # Add option -l giving location for secondary logs
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-logfile");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        # Processing :PARAMPATH: in parameters
        foreach my $param (@{$testCmd->{'pig_params'}}) {
            $param =~ s/:PARAMPATH:/$testCmd->{'paramPath'}/g;
        }
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    push(@cmd, $pigfile);


    # Run the command
    print $log "Setting PIG_CLASSPATH to $ENV{'PIG_CLASSPATH'}\n";
    print $log "$0::$className::$subName INFO: Going to run pig command: @cmd\n";

    IPC::Run::run(\@cmd, \undef, $log, $log) or
        die "Failed running $pigfile\n";
    $result{'rc'} = $? >> 8;


    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    $self->parseSqlQueries($testCmd);
    my @SQLQuery = @{$testCmd->{'queries'}}; # here only used to determine if single-guery of multi-query
       
    # mapreduce
    if($self->{'exectype'} eq "mapred")
    {
        # single query
        if ($#SQLQuery == 0) {
            if ($copyResults) {
              $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
              $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
            } else {
              $result{'output'} = "NO_COPY";
            }
        }
        # multi query
        else {
            my @outfiles = ();
            for (my $id = 1; $id <= ($#SQLQuery + 1); $id++) {
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                $localoutfile = $outfile . ".$id";

                # Copy result file out of hadoop
                my $testOut;
                if ($copyResults) {
                  $testOut = $self->postProcessSingleOutputFile($localoutfile, $localdir, \@baseCmd, $testCmd, $log);
                } else {
                  $testOut = "NO_COPY";
                }
                push(@outfiles, $testOut);
            }
            ##!!! originalOutputs not set! Needed?
            $result{'outputs'} = \@outfiles;
        }
    }
    # local mode
    else 
    {
        # single query
        if ($#SQLQuery == 0) {
            $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".dir";
            mkdir $localdir;
            $result{'output'} = $self->postProcessSingleOutputFile($outfile, $localdir, \@baseCmd, $testCmd, $log);
            $result{'originalOutput'} = "$localdir/out_original"; # populated by postProcessSingleOutputFile
        } 
        # multi query
        else {
            my @outfiles = ();
            for (my $id = 1; $id <= ($#SQLQuery + 1); $id++) {
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
                mkdir $localdir;
                $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out/$id";
                mkdir $localdir;
                $localoutfile = $outfile . ".$id";

                my $testRes = $self->postProcessSingleOutputFile($localoutfile, $localdir, \@baseCmd, $testCmd, $log);
                push(@outfiles, $testRes);
            }
            ##!!! originalOutputs not set!
            $result{'outputs'} = \@outfiles;
        }
    }

    # Compare doesn't get the testCmd hash, so I need to stuff the necessary
    # info about sorting into the result.
    if (defined $testCmd->{'sortArgs'} && $testCmd->{'sortArgs'}) {
        $result{'sortArgs'} = $testCmd->{'sortArgs'};
    }

    return \%result;
}


sub postProcessSingleOutputFile
{
    my ($self, $outfile, $localdir, $baseCmd, $testCmd, $log) = @_;
    my $subName  = (caller(0))[3];

    # Copy to local if results on HDFS
    if ($self->{'exectype'} eq "mapred") {
        my @baseCmd = @{$baseCmd};
        my @copyCmd = @baseCmd;
        push(@copyCmd, ('-e', 'copyToLocal', $outfile, $localdir)); 
        print $log "$0::$className::$subName INFO: Going to run pig command: @copyCmd\n";
 
       IPC::Run::run(\@copyCmd, \undef, $log, $log) or die "Cannot copy results from HDFS $outfile to $localdir\n";
    }


    # Sort the result if necessary.  Keep the original output in one large file.
    # Use system not IPC run so that the '*' gets interpolated by the shell.
    
    # Build command to:
    # 1. Combine part files
    my $fppCmd = 
            ($self->{'exectype'} eq "mapred") ? "cat $localdir/map* $localdir/part* 2>/dev/null" : 
            (-d $outfile )                    ? "cat $outfile/part* 2>/dev/null" :
                                                "cat $outfile";
    
    # 2. Standardize float precision
    if (defined $testCmd->{'floatpostprocess'} && defined $testCmd->{'delimiter'}) {
        $fppCmd .= " | $toolpath/floatpostprocessor.pl '" . $testCmd->{'delimiter'} . "'";
    }
    
    $fppCmd .= " > $localdir/out_original";
    
    # run command
    print $log "$fppCmd\n";
    system($fppCmd);

    # Sort the results for the benchmark compare.
    my @sortCmd = ('sort', "$localdir/out_original");
    print $log join(" ", @sortCmd) . "\n";
    IPC::Run::run(\@sortCmd, '>', "$localdir/out_sorted");

    return "$localdir/out_sorted";
}

sub generateBenchmark
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    $self->parseSqlQueries($testCmd);
    my @SQLQuery = @{$testCmd->{'queries'}};
 
    if ($#SQLQuery == 0) {
        my $outfile = $self->generateSingleSQLBenchmark($testCmd, $SQLQuery[0], undef, $log);
        $result{'output'} = $outfile;
    } else {
        my @outfiles = ();
        for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
            my $sql = $SQLQuery[$id];
            my $outfile = $self->generateSingleSQLBenchmark($testCmd, $sql, ($id+1), $log); 
            push(@outfiles, $outfile);
        }
        $result{'outputs'} = \@outfiles;
    }

    return \%result;
}

sub generateSingleSQLBenchmark
{
    my ($self, $testCmd, $sql, $id, $log) = @_;

    my $qmd5 = substr(md5_hex($testCmd->{'pig'}), 0, 5);
    my $outfile = $testCmd->{'benchmarkPath'} . "/" . $testCmd->{'group'} . "_" . $testCmd->{'num'};
    $outfile .= defined($id) ? ".$id" . ".expected." . $qmd5 :  ".expected." . $qmd5;
    
    if (-e $outfile) {
        return $outfile;
    }

    my @cmd = ('psql', '-U', $testCmd->{'dbuser'}, '-d', $testCmd->{'dbdb'},
        '-c', $sql, '-t', '-A', '--pset', "fieldsep=	", '-o', $outfile);

    print $log "Running SQL command [" . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or do {
        print $log "Sql command <" . $sql .
            " failed for >>$testCmd->{group}_$testCmd->{num}<<\n";
        unlink $outfile if ( -e $outfile  );
    
        die "Sql command failed for >>$testCmd->{group}_$testCmd->{num}<<\n";
    };
    
    # Sort and postprocess the result if necessary
    my $sortCmd = "cat $outfile";
    if (defined $testCmd->{'floatpostprocess'} && defined $testCmd->{'delimiter'}) {
        $sortCmd .= " | $toolpath/floatpostprocessor.pl '" . $testCmd->{'delimiter'} . "' ";
    }
    
    # Use system not IPC run so that the '*' gets interpolated by the shell.
    $sortCmd .= " | sort";
    if (defined $testCmd->{'sortBenchmarkArgs'}) {
        $sortCmd .= " " . join(" ", @{$testCmd->{'sortBenchmarkArgs'}});
    }
    my $tmpfile = $outfile . ".tmp";
    $sortCmd .= " > $tmpfile";
    print $log "$sortCmd\n";
    system($sortCmd);
    unlink $outfile;

    IPC::Run::run ['mv', $tmpfile, $outfile];

    return $outfile;
}

sub hasCommandLineVerifications
{
    my ($self, $testCmd, $log) = @_;

    foreach my $key ('rc', 'expected_out', 'expected_out_regex', 'expected_err', 'expected_err_regex', 
                     'not_expected_out', 'not_expected_out_regex', 'not_expected_err', 'not_expected_err_regex' ) {
      if (defined $testCmd->{$key}) {
         return 1;
      }
    }
    return 0;
}


sub compare
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    # For now, if the test has 
    # - testCmd pig, and 'sql' for benchmark, then use comparePig, i.e. using benchmark
    # - any verification directives formerly used by CmdLine or Script drivers (rc, regex on out and err...)
    #   then use compareScript even if testCmd is "pig"
    # - testCmd script, then use compareScript
    # - testCmd pig, and none of the above, then use comparePig
    #
    # (The first condition included because nightly.conf actually had a test, Types_2, that 
    # specified both 'sql' and 'expected_err_regex'.)
    #
    # Later, should add ability to have same tests both verify with the 'script' directives, 
    # and do a benchmark compare, if it was a pig cmd. E.g. 'rc' could still be checked when 
    # doing the benchmark compare.

    if( defined $testCmd->{'sql'} ){
       return $self->comparePig ( $testResult, $benchmarkResult, $log, $testCmd);
    } elsif( $testCmd->{'script'} || $self->hasCommandLineVerifications( $testCmd, $log) ){
       return $self->compareScript ( $testResult, $log, $testCmd);
    } elsif( $testCmd->{'pig'} ){
       return $self->comparePig ( $testResult, $benchmarkResult, $log, $testCmd);
   #} elsif( $testCmd->{'pigsql'} ){
   #   return $self->comparePigSql ( $testResult, $benchmarkResult, $log, $testCmd);
    } else {
       # Should have been caught by runTest, still...
       print $log "$0.$subName WARNING Did not find a testCmd that I know how to handle\n";
       return 0;
    } 
}


sub compareScript
{
    my ($self, $testResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    # IMPORTANT NOTES:
    #
    # If you are using a regex to compare stdout or stderr
    # and if the pattern that you are trying to match spans two line
    # explicitly use '\n' (without the single quotes) in the regex
    #
    # If any verification directives are added here 
    # do remember also to add them to the hasCommandLineVerifications subroutine.
    #
    # If the test conf file misspells the directive, you won't be told...
    # 

    my $result = 1;  # until proven wrong...


    # Return Code
    if (defined $testCmd->{'rc'}) {                                                                             
      print $log "$0::$subName INFO Checking return code " .
                 "against expected <$testCmd->{'rc'}>\n";
      if ( (! defined $testResult->{'rc'}) || ($testResult->{'rc'} != $testCmd->{'rc'})) {                                                         
        print $log "$0::$subName INFO Check failed: rc = <$testCmd->{'rc'}> expected, test returned rc = <$testResult->{'rc'}>\n";
        $result = 0;
      }
    }

    # Standard Out
    if (defined $testCmd->{'expected_out'}) {
      print $log "$0::$subName INFO Checking test stdout' " .
              "as exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} ne $testCmd->{'expected_out'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_out'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "as NOT exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} eq $testCmd->{'not_expected_out'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_out'}> expected to stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_out_regex'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "for regular expression <$testCmd->{'expected_out_regex'}>\n";
      if ($testResult->{'stdout'} !~ $testCmd->{'expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out_regex'}) {
      print $log "$0::$subName INFO Checking test stdout " .
              "for NON-match of regular expression <$testCmd->{'not_expected_out_regex'}>\n";
      if ($testResult->{'stdout'} =~ $testCmd->{'not_expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    # Standard Error
    if (defined $testCmd->{'expected_err'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "as exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} ne $testCmd->{'expected_err'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_err'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "as NOT an exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} eq $testCmd->{'not_expected_err'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_err'}> expected to stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_err_regex'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "for regular expression <$testCmd->{'expected_err_regex'}>\n";
      if ($testResult->{'stderr'} !~ $testCmd->{'expected_err_regex'}) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err_regex'}) {
      print $log "$0::$subName INFO Checking test stderr " .
              "for NON-match of regular expression <$testCmd->{'not_expected_err_regex'}>\n";
      if ($testResult->{'stderr'} =~ $testCmd->{'not_expected_err_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

  return $result;
}


# sub comparePigSql
# {
#     my ($self, $testResult, $benchmarkResult, $testCmd, $log) = @_;
#     my $subName  = (caller(0))[3];
# 
#     my $result;
# ##!!! STUB
# }


sub comparePig
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd) = @_;
    my $subName  = (caller(0))[3];

    my $result;
    $self->parseSqlQueries($testCmd);
    my @SQLQuery = @{$testCmd->{'queries'}};
    
    if ($#SQLQuery == 0) {
        $result = $self->compareSingleOutput($testResult, $testResult->{'output'},
                $benchmarkResult->{'output'}, $log);
    } else {
        my $res = 0;
        for (my $id = 0; $id < ($#SQLQuery + 1); $id++) {
            my $testOutput = ($testResult->{'outputs'})->[$id];
            my $benchmarkOutput = ($benchmarkResult->{'outputs'})->[$id];
            $res += $self->compareSingleOutput($testResult, $testOutput,
                                               $benchmarkOutput, $log);
            $result = ($res == ($#SQLQuery + 1)) ? 1 : 0;
        }
    }

    return $result;
}


sub compareSingleOutput
{
    my ($self, $testResult, $testOutput, $benchmarkOutput, $log) = @_;

print $log "testResult: $testResult testOutput: $testOutput benchmarkOutput: $benchmarkOutput\n";

    # cksum the the two files to see if they are the same
    my ($testChksm, $benchmarkChksm);
    IPC::Run::run((['cat', $testOutput], '|', ['cksum']), \$testChksm,
        $log) or die "$0: error: cannot run cksum on test results\n";
    IPC::Run::run((['cat', $benchmarkOutput], '|', ['cksum']),
        \$benchmarkChksm, $log) or die "$0: error: cannot run cksum on benchmark\n";

    chomp $testChksm;
    chomp $benchmarkChksm;
    print $log "test cksum: $testChksm\nbenchmark cksum: $benchmarkChksm\n";

    my $result;
    if ($testChksm ne $benchmarkChksm) {
        print $log "Test output checksum does not match benchmark checksum\n";
        print $log "Test checksum = <$testChksm>\n";
        print $log "Expected checksum = <$benchmarkChksm>\n";
        print $log "RESULTS DIFFER: vimdiff $testOutput $benchmarkOutput\n";
    } else {
        $result = 1;
    }

    # Now, check if the sort order is specified
    if (defined($testResult->{'sortArgs'})) {
        Util::setLocale();
	my @sortChk = ('sort', '-cs');
        push(@sortChk, @{$testResult->{'sortArgs'}});
        push(@sortChk, $testResult->{'originalOutput'});
        print $log "Going to run sort check command: " . join(" ", @sortChk) . "\n";
        IPC::Run::run(\@sortChk, \undef, $log, $log);
	my $sortrc = $?;
        if ($sortrc) {
            print $log "Sort check failed\n";
            $result = 0;
        }
    }

    return $result;
}

##############################################################################
# Parse the SQL queries from a string into an array
#
sub parseSqlQueries($$)
{
    my ($self, $testCmd) = @_;

    my @SQLQuery = split /;/, $testCmd->{'sql'};

    # Throw out the last one if it is just space
    if ($SQLQuery[$#SQLQuery] =~ /^\s*$/) { $#SQLQuery--; }

    # If the last one is a comment, decrement the count
    if ($#SQLQuery > 0 && $SQLQuery[$#SQLQuery] !~ /select/i && $SQLQuery[$#SQLQuery] =~ /--/) {
        $#SQLQuery--;
    }

    $testCmd->{'queries'} = \@SQLQuery;
}

###############################################################################
# This method has been copied over from TestDriver to make changes to
# support skipping tests which do not match current execution mode
#
#
# Run all the tests in the configuration file.
# @param testsToRun - reference to array of test groups and ids to run
# @param testsToMatch - reference to array of test groups and ids to match.
# If a test group_num matches any of these regular expressions it will be run.
# @param cfg - reference to contents of cfg file
# @param log - reference to a stream pointer for the logs
# @param dbh - reference database connection
# @param testStatuses- reference to hash of test statuses
# @param confFile - config file name
# @param startat - test to start at.
# @returns nothing
# failed.
#
=begin
sub run
{
    my ($self, $testsToRun, $testsToMatch, $cfg, $log, $dbh, $testStatuses,
        $confFile, $startat, $logname ) = @_;
    my $subName  = (caller(0))[3];

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

    # Do the global setup
    $self->globalSetup(\%globalHash, $log);

    my $report=0;
    my $properties= new Properties( 0, $globalHash{'propertiesFile'} );

    # For the xml directory, use the default directory from the configuration file
    # unless the directory was specified in the command line

    my $xmlDir= $globalHash{'localxmlpathbase'} ."/run".  $globalHash{'UID'};
    if ( $globalHash{'reportdir'} ) {
        $xmlDir = $globalHash{'reportdir'};
    } 

    my %groupExecuted;

    foreach my $group (@{$cfg->{'groups'}}) {
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

            if ( $groupExecuted{ $group->{'name'} }== 0 ){
               $groupExecuted{ $group->{'name'} }=1;

               mkpath( [ $xmlDir ] , 1, 0777) if ( ! -e $xmlDir );

               my $filename = $group->{'name'}.".xml";
               $report = new TestReport ( $properties, "$xmlDir/$filename" );
               $report->purge();

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
            # that failed, or that the test wasn't marked as "ignore".
            # Don't abort if that test wasn't run, just assume the
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
                # if the test is not applicable to current execution mode
                # ignore it
                if(/^exectype$/i && $testHash{$_} !~ /$self->{'exectype'}/i)
                {
                    print $log "\nIGNORING test since running mode ($self->{'exectype'}) and exectype in test ($testHash{'exectype'}) do not match\n";
                    $testStatuses->{$testName} = $skippedStr;
                    $skipThisOne = 1;
                    last;
                }

                # if the test is marked as 'ignore',
                # ignore it... unless option to ignore the ignore is in force
                if(/^ignore$/i && ($self->{'ignore'} eq 'true'))
                {
                    print $log "$0::$className::$subName TEST IGNORED <$testName> at " . time . ". Message: $testHash{'ignore'}\n";
                    $testStatuses->{$testName} = $skippedStr;
                    $skipThisOne = 1;
                    last;
                }
            }

            if ($skipThisOne) {
                printResults($testStatuses, $log, "Results so far");
                next;
            }

            # Check if output comparison should be skipped.
            my $dontCompareThisOne = 0; # true for tests with key 'noverify'
            my $copyResults        = 1; # no need to copy output to local if noverify
            foreach (keys(%testHash)) {

                if(/^noverify$/i )
                {
                    $dontCompareThisOne = 1;
                    $copyResults = 0;
                    last;
                }
            }

            print $log "Beginning test $testName at " . time . "\n";
            my %dbinfo = (
                'testrun_id' => $testHash{'trid'},
                'test_type'  => $testHash{'driver'},
               #'test_file'  => $testHash{'file'},
                'test_file'  => $confFile,
                'test_group' => $testHash{'group'},
                'test_num'   => $testHash{'num'},
            );
            my $beginTime = time;
            my ($testResult, $benchmarkResult);
            my $msg;
            eval {
                

                my  @SQLQuery = split /;/, $testHash{'sql'};

                # Throw out the last one if it is just space
                if ($SQLQuery[$#SQLQuery] =~ /^\s*$/) { $#SQLQuery--; }

                # If the last one is a comment, decrement the count
                if ($#SQLQuery > 0 && $SQLQuery[$#SQLQuery] !~ /select/i && $SQLQuery[$#SQLQuery] =~ /--/) {
                    $#SQLQuery--;
                }

                $testHash{'queries'} = \@SQLQuery;

                $testResult = $self->runTest(\%testHash, $log, $copyResults);
                my $endTime = time;

                $benchmarkResult = $self->generateBenchmark(\%testHash, $log);

                my $result;
                if( $dontCompareThisOne ) {
                    $result = 1;
                    print $log "$0::$className::$subName TEST MARKED NOVERIFY <$testName>\n";
                } else {
                    $result = $self->compare($testResult, $benchmarkResult, $log, \%testHash);
                    print $log "Test $testName\n";
                }

                if ($result) {
                        $msg = "TEST SUCCEEDED <$testName> at " . time . "\n";
                        $testStatuses->{$testName} = $passedStr;
                } else {
                        $msg= "TEST FAILED <$testName> at " . time . "\n";
                        $testStatuses->{$testName} = $failedStr;
                }
                print $log $msg;

                $dbinfo{'duration'} = $endTime - $beginTime;
                $self->recordResults($result, $testResult, $benchmarkResult,
                    \%dbinfo, $log);
            };

            if ($@) {
                my $endTime = time;
                print $log "$0::$subName FAILED: Failed to run test $testName <$@>\n";
                $testStatuses->{$testName} = $abortedStr;
                $dbinfo{'duration'} = $beginTime - $endTime;
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
            printResults($testStatuses, $log, "Results so far");
        }
           if ( $report ) {
              my $reportname= $group->{'name'};
              if ( $globalHash{'reportname'} ) {
                 $reportname= $globalHash{'reportname'};
              }
              $report->systemOut( $logname, $reportname );
              printGroupResultsXml( $report, $reportname, $testStatuses, $groupDuration );
           }
           $report = 0;
           $groupDuration=0;

    }

    # Do the global cleanup
    $self->globalCleanup(\%globalHash, $log);
}
=cut

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

1;
