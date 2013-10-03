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
use Cwd;
use Data::Dumper;

use strict;
use English;

our $className= "TestDriver";
our @ISA = "$className";
our $ROOT = (defined $ENV{'HARNESS_ROOT'} ? $ENV{'HARNESS_ROOT'} : die "ERROR: You must set environment variable HARNESS_ROOT\n");
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

    bless($self, $class);
    return $self;
}

sub replaceParameters
{
##!!! Move this to Util.pm

    my ($self, $cmd, $outfile, $testCmd, $log, $resources) = @_;

    # $self
    $cmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    # $outfile
    $cmd =~ s/:OUTPATH:/$outfile/g;

    # $ENV
    $cmd =~ s/:PIGHARNESS:/$ENV{HARNESS_ROOT}/g;

    # $testCmd
    $cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
    $cmd =~ s/:OUTPATH:/$outfile/g;
    $cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
    $cmd =~ s/:PIGGYBANKPATH:/$testCmd->{'piggybankjarPath'}/g;
    $cmd =~ s/:PIGPATH:/$testCmd->{'pigpath'}/g;
    $cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
    $cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
    $cmd =~ s/:MAPREDJARS:/$testCmd->{'mapredjars'}/g;
    $cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
    $cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
    $cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
#    $cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
#    $cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
    $cmd =~ s/:LOCALTESTPATH:/$testCmd->{'localpathbase'}/g;
    $cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
    $cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
    $cmd =~ s/:HDFSTMP:/tmp\/$testCmd->{'runid'}/g;

    if ( $testCmd->{'hadoopSecurity'} eq "secure" ) { 
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteSecureCluster'}/g;
    } else {
      $cmd =~ s/:REMOTECLUSTER:/$testCmd->{'remoteNotSecureCluster'}/g;
    }

    if ( defined($testCmd->{'hcatbin'}) && $testCmd->{'hcatbin'} ne "" && defined($testCmd->{'java_params'})) {
      foreach my $param (@{$testCmd->{'java_params'}}) {
          $param =~ s/:HCATBIN:/$testCmd->{'hcatbin'}/g;
      }
    }

    foreach (keys(%$resources)) {
        $cmd =~ s/:$_:/$resources->{$_}/g;
    }

    return $cmd;
}

sub globalSetup
{
    my ($self, $globalHash, $log) = @_;

    # Setup the output path
    my $me = `whoami`;
    chomp $me;
    my $jobId = $globalHash->{'job-id'};
    my $timeId = time;
    $globalHash->{'runid'} = $me . "-" . $timeId . "-" . $jobId;

    # if "-ignore false" was provided on the command line,
    # it means do run tests even when marked as 'ignore'
    if(defined($globalHash->{'ignore'}) && $globalHash->{'ignore'} eq 'false')
    {
        $self->{'ignore'} = 'false';
    }

    $globalHash->{'outpath'} = $globalHash->{'outpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'localpath'} = $globalHash->{'localpathbase'} . "/" . $globalHash->{'runid'} . "/";
    $globalHash->{'tmpPath'} = $globalHash->{'tmpPath'} . "/" . $globalHash->{'runid'} . "/";
}

sub globalSetupConditional() {
    my ($self, $globalHash, $log) = @_;

    # add libexec location to the path
    if (defined($ENV{'PATH'})) {

        #detect os and modify path accordingly
        if(Util::isWindows()) {
            $ENV{'PATH'} = $globalHash->{'scriptPath'} . ";" . $ENV{'PATH'};
        }
        else {
            $ENV{'PATH'} = $globalHash->{'scriptPath'} . ":" . $ENV{'PATH'};
        }
    } else {
        $ENV{'PATH'} = $globalHash->{'scriptPath'};
    }

    my @cmd = ($self->getPigCmd($globalHash, $log), '-e', 'mkdir', $globalHash->{'outpath'});
	print $log "Going to run " . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or die "$0 at ".__LINE__.": Cannot create HDFS directory " . $globalHash->{'outpath'} . ": $? - $!\n";

    File::Path::make_path(
            $globalHash->{'localpath'},
            $globalHash->{'tmpPath'});

    # Create the HDFS temporary directory
    @cmd = ($self->getPigCmd($globalHash, $log), '-e', 'mkdir', "tmp/$globalHash->{'runid'}");
	print $log "Going to run " . join(" ", @cmd) . "\n";
    IPC::Run::run(\@cmd, \undef, $log, $log) or die "$0 at ".__LINE__.": Cannot create HDFS directory " . "tmp/$globalHash->{'runid'}" . ": $? - $!\n";
}

sub globalCleanup()
{
    # noop there because the removal of temp directories, which are created in #globalSetupConditional(), is to be
    # performed in method #globalCleanupConditional().
}

sub globalCleanupConditional() {
    my ($self, $globalHash, $log) = @_;

    # NB: both local and HDFS output directories are not removed there, because these data may 
    # be needed to investigate the tests failures.

    IPC::Run::run(['rm', '-rf', $globalHash->{'tmpPath'}], \undef, $log, $log) or 
       warn "Cannot remove temporary directory " . $globalHash->{'tmpPath'} .
           " " . "$ERRNO\n";

    # Cleanup the HDFS temporary directory
    my @cmd = ($self->getPigCmd($globalHash, $log), '-e', 'fs', '-rmr', "tmp/$globalHash->{'runid'}");
    print $log "Going to run: [" . join(" ", @cmd) . "]\n";
    IPC::Run::run(\@cmd, \undef, $log, $log)
       or die "$0 at ".__LINE__.": Cannot remove HDFS directory " . "tmp/$globalHash->{'runid'}" . ": $? - $!\n";
}

sub runTest
{
    my ($self, $testCmd, $log, $resources) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    if ($self->wrongExecutionMode($testCmd, $log)) {
        my %result;
        return \%result;
    }

    # Handle the various methods of running used in 
    # the original TestDrivers

    if ( $testCmd->{'pig'} && $self->hasCommandLineVerifications( $testCmd, $log) ) {
       my $oldpig;

       if ( Util::isWindows() && $testCmd->{'pig_win'}) {
           $oldpig = $testCmd->{'pig'};
           $testCmd->{'pig'} = $testCmd->{'pig_win'};
       }

       if ( $testCmd->{'hadoopversion'} == '23' && $testCmd->{'pig23'}) {
           $oldpig = $testCmd->{'pig'};
           $testCmd->{'pig'} = $testCmd->{'pig23'};
       }
       if ( $testCmd->{'hadoopversion'} == '23' && $testCmd->{'expected_err_regex23'}) {
           $testCmd->{'expected_err_regex'} = $testCmd->{'expected_err_regex23'};
       }
       my $res = $self->runPigCmdLine( $testCmd, $log, 1, $resources );
       if ($oldpig) {
           $testCmd->{'pig'} = $oldpig;
       }
       return $res;
    } elsif( $testCmd->{'pig'} ){
       my $oldpig;

       if ( Util::isWindows() && $testCmd->{'pig_win'}) {
           $oldpig = $testCmd->{'pig'};
           $testCmd->{'pig'} = $testCmd->{'pig_win'};
       }

       if ( $testCmd->{'hadoopversion'} == '23' && $testCmd->{'pig23'}) {
           $oldpig = $testCmd->{'pig'};
           $testCmd->{'pig'} = $testCmd->{'pig23'};
       }
       my $res = $self->runPig( $testCmd, $log, 1, $resources );
       if ($oldpig) {
           $testCmd->{'pig'} = $oldpig;
       }
       return $res;
    } elsif(  $testCmd->{'script'} ){
       return $self->runScript( $testCmd, $log, $resources );
    } else {
       die "$subName FATAL Did not find a testCmd that I know how to handle";
    }
}


sub runPigCmdLine
{
    my ($self, $testCmd, $log, $copyResults, $resources) = @_;
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
    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log, $resources );

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

    if (defined($testCmd->{'additional_cmd_args'})) {
        push(@cmd, $testCmd->{'additional_cmd_args'});
    }
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
    my ($self, $testCmd, $log, $resources) = @_;
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
    my $cmd = $self->replaceParameters( $testCmd->{'script'}, $outfile, $testCmd, $log, $resources );

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

sub hadoopLocalTmpDir($$)
{
    my ($self, $testCmd) = @_;

    if (defined($testCmd->{'hadoop.mapred.local.dir'}) 
         && (int($ENV{'FORK_FACTOR_GROUP'})>1 || int($ENV{'FORK_FACTOR_FILE'})>1)) {
        return $testCmd->{'hadoop.mapred.local.dir'} . "/" . $PID;
    } else {
        return undef;
    }
}

sub getPigCmd($$$)
{
    my ($self, $testCmd, $log) = @_;

    my @pigCmd;

    # set the PIG_CLASSPATH environment variable
	my $separator = ":";
	if(Util::isWindows()) {
	    $separator = ";";
	}
	my $pcp .= $testCmd->{'jythonjar'} if (defined($testCmd->{'jythonjar'}));
    $pcp .= $separator . $testCmd->{'jrubyjar'} if (defined($testCmd->{'jrubyjar'}));
    $pcp .= $separator . $testCmd->{'classpath'} if (defined($testCmd->{'classpath'}));

    # Set it in our current environment.  It will get inherited by the IPC::Run
    # command.
    $ENV{'PIG_CLASSPATH'} = $pcp;

    if ($testCmd->{'usePython'} eq "true") {
        @pigCmd = ("python");
        push(@pigCmd, "$testCmd->{'pigpath'}/bin/pig.py");
        # print ("Using pig too\n");
    } else {
        if(Util::isWindows()) {
            @pigCmd = ("$testCmd->{'pigpath'}/bin/pig.cmd");
        }
        else {
           @pigCmd = ("$testCmd->{'pigpath'}/bin/pig");
        }
    }

    if (defined($testCmd->{'additionaljars'})) {
        push(@pigCmd, '-Dpig.additional.jars='.$testCmd->{'additionaljars'});
    }

    my $additionalJavaParams = undef;
    if ($testCmd->{'exectype'} eq "local") {
        $additionalJavaParams = "-Xmx1024m";
        my $hadoopTmpDir = $self->hadoopLocalTmpDir($testCmd);
        if (defined($hadoopTmpDir)) {
           $additionalJavaParams .= " -Dmapred.local.dir=$hadoopTmpDir -Dmapreduce.cluster.local.dir=$hadoopTmpDir";
        }
        TestDriver::dbg("Additional java parameters: [$additionalJavaParams].\n");

        push(@pigCmd, ("-x", "local"));
    }

    if (defined($testCmd->{'java_params'}) || defined($additionalJavaParams)) {
        if (defined($testCmd->{'java_params'})) {
	   $ENV{'PIG_OPTS'} = join(" ", @{$testCmd->{'java_params'}}, $additionalJavaParams);
        } else {
           $ENV{'PIG_OPTS'} = $additionalJavaParams;
        }
        TestDriver::dbg("PIG_OPTS set to be: [$ENV{'PIG_OPTS'}].\n");
    } else {
        $ENV{'PIG_OPTS'} = undef;
    }

        if (defined($ENV{'HADOOP_HOME'}) && $ENV{'HADOOP_HOME'} ne "") {
            print $log "HADOOP_HOME=" . $ENV{'HADOOP_HOME'} . "\n";
        }
        if (defined($ENV{'HADOOP_CONF_DIR'}) && $ENV{'HADOOP_CONF_DIR'} ne "") {
            print $log "HADOOP_CONF_DIR=" . $ENV{'HADOOP_CONF_DIR'} . "\n";
        }
        if (defined($ENV{'HADOOP_PREFIX'}) && $ENV{'HADOOP_PREFIX'} ne "") {
            print $log "HADOOP_PREFIX=" . $ENV{'HADOOP_PREFIX'} . "\n";
        }
        if (defined($ENV{'HADOOP_COMMON_HOME'}) && $ENV{'HADOOP_COMMON_HOME'} ne "") {
            print $log "HADOOP_COMMON_HOME=" . $ENV{'HADOOP_COMMON_HOME'} . "\n";
        }
        if (defined($ENV{'HADOOP_HDFS_HOME'}) && $ENV{'HADOOP_HDFS_HOME'} ne "") {
            print $log "HADOOP_HDFS_HOME=" . $ENV{'HADOOP_HDFS_HOME'} . "\n";
        }
        if (defined($ENV{'HADOOP_MAPRED_HOME'}) && $ENV{'HADOOP_MAPRED_HOME'} ne "") {
            print $log "HADOOP_MAPRED_HOME=" . $ENV{'HADOOP_MAPRED_HOME'} . "\n";
        }
        if (defined($ENV{'YARN_HOME'}) && $ENV{'YARN_HOME'} ne "") {
            print $log "YARN_HOME=" . $ENV{'YARN_HOME'} . "\n";
        }
        if (defined($ENV{'YARN_CONF_DIR'}) && $ENV{'YARN_CONF_DIR'} ne "") {
            print $log "YARN_CONF_DIR=" . $ENV{'YARN_CONF_DIR'} . "\n";
        }
	print $log "PIG_CLASSPATH=" . $ENV{'PIG_CLASSPATH'} . "\n";
        print $log "PIG_OPTS=" .$ENV{'PIG_OPTS'} . "\n";
    return @pigCmd;
}



sub runPig
{
    my ($self, $testCmd, $log, $copyResults, $resources) = @_;
    my $subName  = (caller(0))[3];

    my %result;

    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";

    my $pigcmd = $self->replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log, $resources );

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

    if (defined($testCmd->{'additional_cmd_args'})) {
        push(@cmd, @{$testCmd->{'additional_cmd_args'}});
    }

    # Run the command
    print $log "$0::$className::$subName INFO: Going to run pig command: @cmd\n";

    IPC::Run::run(\@cmd, \undef, $log, $log) or
        die "Failed running $pigfile\n";
    $result{'rc'} = $? >> 8;


    # Get results from the command locally
    my $localoutfile;
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $stores = $self->countStores($testCmd);
       
    # single query
    if ($stores == 1) {
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
        for (my $id = 1; $id <= ($stores); $id++) {
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

    my @baseCmd = @{$baseCmd};
    my @copyCmd = @baseCmd;
    push(@copyCmd, ('-e', 'copyToLocal', $outfile, $localdir)); 
    print $log "$0::$className::$subName INFO: Going to run pig command: @copyCmd\n";
 
    IPC::Run::run(\@copyCmd, \undef, $log, $log) or die "Cannot copy results from HDFS $outfile to $localdir\n";


    # Sort the result if necessary.  Keep the original output in one large file.
    # Use system not IPC run so that the '*' gets interpolated by the shell.
    
    # Build command to:
    # 1. Combine part files
    my $fppCmd;
    if(Util::isWindows()) {
        my $delCmd = "del \"$localdir\\*.crc\"";
        print $log "$delCmd\n";
        system($delCmd);
        $fppCmd = "cat $localdir\\map* $localdir\\part* 2>NUL";
    }
    else {
        $fppCmd = "cat $localdir/map* $localdir/part* 2>/dev/null";
    }
    
    # 2. Standardize float precision
    if (defined $testCmd->{'floatpostprocess'} &&
            defined $testCmd->{'delimiter'}) {
        $fppCmd .= " | perl $toolpath/floatpostprocessor.pl \"" .
            $testCmd->{'delimiter'} . "\"";
    }
    
    $fppCmd .= " > $localdir/out_original";
    
    # run command
    print $log "$fppCmd\n";
    system($fppCmd);

    # Sort the results for the benchmark compare.
    my @sortCmd = ('sort', "$localdir/out_original");
    print $log join(" ", @sortCmd) . "\n";
    IPC::Run::run(\@sortCmd, '>', "$localdir/out_sorted") or die "Sort for benchmark comparison failed on $localdir/out_original";

    return "$localdir/out_sorted";
}

sub generateBenchmark
{
    my ($self, $testCmd, $log) = @_;

    my %result;

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    if ($self->wrongExecutionMode($testCmd, $log)) {
        return \%result;
    }

    if ($self->hasCommandLineVerifications($testCmd, $log)) {
        # Do nothing, no benchmark to geneate
        return \%result;
    }

    # If they specified an alternate Pig Latin script, use that on the current
    # version.  Otherwise use a previous version of Pig.
	my %modifiedTestCmd = %{$testCmd};
        my $orighadoophome;
        my $orighadoopconf;
        my $orighadoopprefix;
        my $orighadoopcommonhome;
        my $orighadoophdfshome;
        my $orighadoopmapredhome;
        my $orighadoopyarnhome;
        my $orighadoopyarnconf;

	if (defined $testCmd->{'verify_pig_script'}) {
		$modifiedTestCmd{'pig'} = $testCmd->{'verify_pig_script'};
	}
    else {
        if ( Util::isWindows() && $testCmd->{'pig_win'}) {
           $modifiedTestCmd{'pig'} = $testCmd->{'pig_win'};
       }
		# Change so we're looking at the old version of Pig
		$modifiedTestCmd{'pigpath'} = $testCmd->{'oldpigpath'};
                if (defined($testCmd->{'oldconfigpath'})) {
		    $modifiedTestCmd{'testconfigpath'} = $testCmd->{'oldconfigpath'};
                }
                # switch environment to old hadoop
                $orighadoophome=$ENV{'HADOOP_HOME'};
                $orighadoopconf=$ENV{'HADOOP_CONF_DIR'};
                $orighadoopprefix = $ENV{'HADOOP_PREFIX'};
                $orighadoopcommonhome = $ENV{'HADOOP_COMMON_HOME'};
                $orighadoophdfshome = $ENV{'HADOOP_HDFS_HOME'};
                $orighadoopmapredhome = $ENV{'HADOOP_MAPRED_HOME'};
                $orighadoopyarnhome = $ENV{'YARN_HOME'};
                $orighadoopyarnconf = $ENV{'YARN_CONF_DIR'};

                if (defined($ENV{'OLD_HADOOP_HOME'}) && $ENV{'OLD_HADOOP_HOME'} ne "") {
                    $ENV{'HADOOP_HOME'} = $ENV{'OLD_HADOOP_HOME'};
                    $ENV{'HADOOP_CONF_DIR'} = $ENV{'PH_OLD_CLUSTER_CONF'};
                    $ENV{'HADOOP_PREFIX'} = $ENV{'OLD_HADOOP_PREFIX'};
                    $ENV{'HADOOP_COMMON_HOME'} = $ENV{'OLD_HADOOP_COMMON_HOME'};
                    $ENV{'HADOOP_HDFS_HOME'} = $ENV{'OLD_HADOOP_HDFS_HOME'};
                    $ENV{'HADOOP_MAPRED_HOME'} = $ENV{'OLD_HADOOP_MAPRED_HOME'};
                    $ENV{'YARN_HOME'} = $ENV{'OLD_YARN_HOME'};
                    $ENV{'YARN_CONF_DIR'} = $ENV{'OLD_YARN_CONF_DIR'};
                }
	}
	# Modify the test number so we don't run over the actual test output
	# and logs
	$modifiedTestCmd{'num'} = $testCmd->{'num'} . "_benchmark";

        my $res;
        if (defined $testCmd->{'benchmarkcachepath'} && $testCmd->{'benchmarkcachepath'} ne "") {
           $modifiedTestCmd{'localpath'} = $testCmd->{'benchmarkcachepath'} . "/";
           my $statusFile = $modifiedTestCmd{'localpath'} . $modifiedTestCmd{'group'} . "_" . $modifiedTestCmd{'num'} . ".runPigResult";
           if (open my $in, '<', $statusFile) {
              {
                 local $/;  
                 eval <$in>;
                 print $log "Using existing benchmark: ". Dumper($res) . "\n";
              }
              close $in;
           }
        }

        # run pig if we don't already have the benchmark
	$res = $res || $self->runPig(\%modifiedTestCmd, $log, 1);

        if (defined $testCmd->{'benchmarkcachepath'} && $testCmd->{'benchmarkcachepath'} ne "") {
           # save runPig result along with the files
           my $statusFile = $modifiedTestCmd{'localpath'} . $modifiedTestCmd{'group'} . "_" . $modifiedTestCmd{'num'} . ".runPigResult";
           open my $out, '>', $statusFile or die $!;
           print {$out} Data::Dumper->Dump([$res], ["res"]), $/;
           close $out;
        }

        if (!defined $testCmd->{'verify_pig_script'}) {
                $ENV{'HADOOP_HOME'} = $orighadoophome;
                $ENV{'HADOOP_CONF_DIR'} = $orighadoopconf;
                $ENV{'HADOOP_PREFIX'} = $orighadoopprefix;
                $ENV{'HADOOP_COMMON_HOME'} = $orighadoopcommonhome;
                $ENV{'HADOOP_HDFS_HOME'} = $orighadoophdfshome;
                $ENV{'HADOOP_MAPRED_HOME'} = $orighadoopmapredhome;
                $ENV{'YARN_HOME'} = $orighadoopyarnhome;
                $ENV{'YARN_CONF_DIR'} = $orighadoopyarnconf;
        }

        return $res;
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
    my ($self, $testResult, $benchmarkResult, $log, $testCmd, $resources) = @_;
    my $subName  = (caller(0))[3];

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    if ($self->wrongExecutionMode($testCmd, $log)) {
        # Special magic value
        return $self->{'wrong_execution_mode'}; 
    }

    # For now, if the test has 
    # - testCmd pig, and 'sql' for benchmark, then use comparePig, i.e. using benchmark
    # - any verification directives formerly used by CmdLine or Script drivers (rc, regex on out and err...)
    #   then use compareScript even if testCmd is "pig"
    # - testCmd script, then use compareScript
    # - testCmd pig, and none of the above, then use comparePig
    #
    # Later, should add ability to have same tests both verify with the 'script' directives, 
    # and do a benchmark compare, if it was a pig cmd. E.g. 'rc' could still be checked when 
    # doing the benchmark compare.

    if ( $testCmd->{'script'} || $self->hasCommandLineVerifications( $testCmd, $log) ){
       return $self->compareScript ( $testResult, $log, $testCmd, $resources);
    } elsif( $testCmd->{'pig'} ){
       return $self->comparePig ( $testResult, $benchmarkResult, $log, $testCmd, $resources);
    } else {
       # Should have been caught by runTest, still...
       print $log "$0.$subName WARNING Did not find a testCmd that I know how to handle\n";
       return 0;
    } 
}


sub compareScript
{
    my ($self, $testResult, $log, $testCmd, $resources) = @_;
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
      $testCmd->{'expected_out'} = $self->replaceParameters( $testCmd->{'expected_out'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stdout' " .
              "as exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} ne $testCmd->{'expected_out'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_out'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out'}) {
      $testCmd->{'not_expected_out'} = $self->replaceParameters( $testCmd->{'not_expected_out'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stdout " .
              "as NOT exact match against expected <$testCmd->{'expected_out'}>\n";
      if ($testResult->{'stdout'} eq $testCmd->{'not_expected_out'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_out'}> expected to stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_out_regex'}) {
      $testCmd->{'expected_out_regex'} = $self->replaceParameters( $testCmd->{'expected_out_regex'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stdout " .
              "for regular expression <$testCmd->{'expected_out_regex'}>\n";
      if ($testResult->{'stdout'} !~ $testCmd->{'expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_out_regex'}) {
      $testCmd->{'not_expected_out_regex'} = $self->replaceParameters( $testCmd->{'not_expected_out_regex'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stdout " .
              "for NON-match of regular expression <$testCmd->{'not_expected_out_regex'}>\n";
      if ($testResult->{'stdout'} =~ $testCmd->{'not_expected_out_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_out_regex'}> expected in stdout: $testResult->{'stdout'}\n";
        $result = 0;
      }
    } 

    # Standard Error
    if (defined $testCmd->{'expected_err'}) {
      $testCmd->{'expected_err'} = $self->replaceParameters( $testCmd->{'expected_err'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stderr " .
              "as exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} ne $testCmd->{'expected_err'}) {
        print $log "$0::$subName INFO Check failed: exact match of <$testCmd->{'expected_err'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err'}) {
      $testCmd->{'not_expected_err'} = $self->replaceParameters( $testCmd->{'not_expected_err'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stderr " .
              "as NOT an exact match against expected <$testCmd->{'expected_err'}>\n";
      if ($testResult->{'stderr'} eq $testCmd->{'not_expected_err'}) {
        print $log "$0::$subName INFO Check failed: NON-match of <$testCmd->{'expected_err'}> expected to stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'expected_err_regex'}) {
      $testCmd->{'expected_err_regex'} = $self->replaceParameters( $testCmd->{'expected_err_regex'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stderr " .
              "for regular expression <$testCmd->{'expected_err_regex'}>\n";
      if ($testResult->{'stderr'} !~ $testCmd->{'expected_err_regex'}) {
        print $log "$0::$subName INFO Check failed: regex match of <$testCmd->{'expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

    if (defined $testCmd->{'not_expected_err_regex'}) {
      $testCmd->{'not_expected_err_regex'} = $self->replaceParameters( $testCmd->{'not_expected_err_regex'}, "", $testCmd, $log, $resources );
      print $log "$0::$subName INFO Checking test stderr " .
              "for NON-match of regular expression <$testCmd->{'not_expected_err_regex'}>\n";
      if ($testResult->{'stderr'} =~ $testCmd->{'not_expected_err_regex'}) {
        print $log "$0::$subName INFO Check failed: regex NON-match of <$testCmd->{'not_expected_err_regex'}> expected in stderr: $testResult->{'stderr_file'}\n";
        $result = 0;
      }
    } 

  return $result;
}


sub comparePig
{
    my ($self, $testResult, $benchmarkResult, $log, $testCmd, $resources) = @_;
    my $subName  = (caller(0))[3];

    my $result;
    my $stores = $self->countStores($testCmd);
    
    if ($stores == 1) {
        $result = $self->compareSingleOutput($testResult, $testResult->{'output'},
                $benchmarkResult->{'output'}, $log);
    } else {
        my $res = 0;
        for (my $id = 0; $id < ($stores); $id++) {
            my $testOutput = ($testResult->{'outputs'})->[$id];
            my $benchmarkOutput = ($benchmarkResult->{'outputs'})->[$id];
            $res += $self->compareSingleOutput($testResult, $testOutput,
                                               $benchmarkOutput, $log);
            $result = ($res == ($stores)) ? 1 : 0;
        }
    }

    return $result;
}


sub compareSingleOutput
{
    my ($self, $testResult, $testOutput, $benchmarkOutput, $log) = @_;

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
        print $log "RESULTS DIFFER: vimdiff " . cwd . "/$testOutput $benchmarkOutput\n";
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
# Count the number of stores in a Pig Latin script, so we know how many files
# we need to compare.
#
sub countStores($$)
{
    my ($self, $testCmd) = @_;

    # Special work around for queries with more than one store that are not
    # actually multiqueries.
    if (defined $testCmd->{'notmq'}) {
        return 1;
    }

    my $count;

    # hope they don't have more than store per line
    # also note that this won't work if you comment out a store
    my @q = split(/\n/, $testCmd->{'pig'});
        for (my $i = 0; $i < @q; $i++) {
            $count += $q[$i] =~ /store\s+(\$)?[a-zA-Z][a-zA-Z0-9_]*\s+into/i;
    }

    return $count;
}

##############################################################################
# Check whether we should be running this test or not.
#
sub wrongExecutionMode($$)
{
    my ($self, $testCmd, $log) = @_;

    # Check that we should run this test.  If the current execution type
    # doesn't match the execonly flag, then skip this one.
    my $wrong = ((defined $testCmd->{'execonly'} &&
            $testCmd->{'execonly'} ne $testCmd->{'exectype'}));

    if ($wrong) {
        print $log "Skipping test $testCmd->{'group'}" . "_" .
            $testCmd->{'num'} . " since it is executed only in " .
            $testCmd->{'execonly'} . " mode and we are executing in " .
            $testCmd->{'exectype'} . " mode.\n";
        return $wrong;
    }

    if (defined $testCmd->{'ignore23'} && $testCmd->{'hadoopversion'}=='23') {
        $wrong = 1;
    }

    if ($wrong) {
        print $log "Skipping test $testCmd->{'group'}" . "_" .
            $testCmd->{'num'} . " since it is not suppsed to be run in hadoop 23\n";
    }

    return  $wrong;
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

1;
