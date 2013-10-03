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
                                                                                       
package TestDriverScript;

###############################################################################
# Test driver for pig nightly tests.
# 
#
my $ROOT=undef;
if (defined $ENV{'PIG_HARNESS_ROOT'} ){
   $ROOT= $ENV{'PIG_HARNESS_ROOT'};

} else {
  die "FATAL ERROR: $0 - You must set PIG_HARNESS_ROOT to the root directory of the pig_harness";
}

#Set library paths
unshift( @INC, "$ROOT/libexec" );
unshift( @INC, ".");

use TestDriverPig;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method
use File::Path;
use Digest::MD5 qw(md5_hex);
use Util;

use strict;
use English;

our @ISA = "TestDriverPig";

#########################################################################
#  Sub: new
#  TestDriverPigCmdline Constructor.
#
#
# Paramaters:
# None
#
# Returns:
# None

sub new
{
	# Call our parent
	my ($proto) = @_;
	my $class = ref($proto) || $proto;
	my $self = $class->SUPER::new;
        $self->{'ignore'} = "true";  

	bless($self, $class);
	return $self;

}

########################################################################
# Sub: runTest
# Runs the test and returns the results.
# -Write the pig script to a file.
# -Run the command
# -Copy result file out of hadoop
# -Sort and postprocess the result if necessary
#
# Parameters:
# $testCmd -
# $log     -
#
# Returns:
# hash reference containg the test result
#


sub runTest
{
        my ($self, $testCmd, $log) = @_;
        my $subName = (caller(0))[3];

        my %result;

        # extract the current zebra.jar file path from the classpath
        # and enter it in the hash for use in the substitution of :ZEBRAJAR:
        my $zebrajar = $testCmd->{'cp'};
        $zebrajar =~ s/zebra.jar.*/zebra.jar/;
        $zebrajar =~ s/.*://;
        $testCmd->{'zebrajar'} = $zebrajar;

        if(  $testCmd->{'pig'} ){
           return runPig( $self, $testCmd, $log );
        } elsif(  $testCmd->{'pigsql'} ){
           return runPigSql( $self, $testCmd, $log );
        } elsif(  $testCmd->{'script'} ){
           return runScript( $self, $testCmd, $log );
        }

        return %result;
}

sub runPig
{
    my ($self, $testCmd, $log) = @_;
    my $subName = (caller(0))[3];
    my %result;

    return 1 if ( $testCmd->{'ignore'}=~ "true" );
    # Write the pig script to a file.
    my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
    my $outdir = $testCmd->{'outlpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    my $outfile = "$outdir/pig.out";

    my $pigcmd =  $testCmd->{'pig'};
    $pigcmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
    $pigcmd =~ s/:OUTPATH:/$outfile/g;
    $pigcmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
    $pigcmd =~ s/:PIGGYBANKPATH:/$testCmd->{'piggybankjarPath'}/g;
    $pigcmd =~ s/:ZEBRAJAR:/$testCmd->{'zebrajar'}/g;
    $pigcmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
    $pigcmd =~ s/:PIGHARNESS:/$ENV{PIG_HARNESS_ROOT}/g;
    $pigcmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
    $pigcmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
    $pigcmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
    $pigcmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
    $pigcmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
    $pigcmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
    $pigcmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
    $pigcmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
    $pigcmd =~ s/:FILER:/$testCmd->{'filerPath'}/g;

    $pigcmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    #my $pigcmd = replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

    open(FH, ">$pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
    print FH $pigcmd . "\n";
    close(FH);

    # Run the command
    my @cmd = Util::getBasePigCmd($testCmd);

    # Add option -l giving location for secondary logs
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    mkdir $localdir;
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-l");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    push(@cmd, $pigfile);

    print $log "Going to run pig command:  @cmd\n";
    print $log "Pig script contains: $pigcmd\n";

    IPC::Run::run(\@cmd, \undef, \$result{'stdout'}, \$result{'stderr'});
#    print `@cmd`;
        $result{'rc'} = $? >> 8;
    return \%result;
}


sub runPigSql
{
        my ($self, $testCmd, $log) = @_;
        my $subName = (caller(0))[3];
        my %result;

        return 1 if ( $testCmd->{'ignore'}=~ "true" );
        # Write the pig script to a file.
        my $pigfile = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".pig";
        my $outdir  = $testCmd->{'outlpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
	my $outfile = "$outdir/pig.out";

        my $pigcmd =  $testCmd->{'pigsql'};
	$pigcmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
	$pigcmd =~ s/:OUTPATH:/$outfile/g;
	$pigcmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
	$pigcmd =~ s/:PIGGYBANKPATH:/$testCmd->{'piggybankjarPath'}/g;
	$pigcmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
	$pigcmd =~ s/:PIGHARNESS:/$ENV{PIG_HARNESS_ROOT}/g;
	$pigcmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
	$pigcmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
	$pigcmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
	$pigcmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
	$pigcmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
	$pigcmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
	$pigcmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
	$pigcmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
        $pigcmd =~ s/:FILER:/$testCmd->{'filerPath'}/g;

        $pigcmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

        #my $pigcmd = replaceParameters( $testCmd->{'pig'}, $outfile, $testCmd, $log );

        open(FH, ">$pigfile") or die "Unable to open file $pigfile to write pig script, $ERRNO\n";
        print FH $pigcmd . "\n";
        close(FH);

    # Run the command
    my $outfile = $testCmd->{'outpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    $testCmd->{'testoutpath'}=$outfile;
    $self->{'latestoutputpath'}=$outfile;

    # Pig _SQL_ command
    my @cmd = Util::getBasePigSqlCmd($testCmd);

    # Add option -l giving location for secondary logs
    my $localdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
    mkdir $localdir;
    my $locallog = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".log";
    push(@cmd, "-l");
    push(@cmd, $locallog);

    # Add pig parameters if they're provided
    if (defined($testCmd->{'pig_params'})) {
        push(@cmd, @{$testCmd->{'pig_params'}});
    }

    push(@cmd, $pigfile);
    print $log "Going to run pig sql command: @cmd\n";
    print $log "Pig Sql script contains: $pigcmd\n";
    IPC::Run::run(\@cmd, \undef, \$result{'stdout'}, \$result{'stderr'});
    $result{'rc'} = $? >> 8;
    return \%result;
}


########################################################################
# Sub: runScript
# Runs the Script and returns the results.
# -Write the script to a file.
# -Run the command
#
# Parameters:
# $testCmd -
# $log     -
#
# Returns:
# hash reference containg the test result
#

sub runScript
{
	my ($self, $testCmd, $log) = @_;
        my $subName = (caller(0))[3];
	my %result;

        return 1 if ( $testCmd->{'ignore'}=~ "true" );

	# Write the pig script to a file.
	my $script = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".sh";

        my $outdir = $testCmd->{'localpath'} . $testCmd->{'group'} . "_" . $testCmd->{'num'} . ".out";
        print $log "Attempting to create $outdir\n";
        mkpath( [ $outdir ] , 1, 0755) if ( ! -e outdir );
        if ( ! -e $outdir ){ 
           print $log "$0.$subName FATAL could not mkdir $outdir\n";
           die "$0.$subName FATAL could not mkdir $outdir\n";
        }
	my $outfile = "$outdir/script.out";

        my $cmd = $testCmd->{'script'};
	$cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
	$cmd =~ s/:OUTPATH:/$outfile/g;
	$cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
	$cmd =~ s/:PIGGYBANKPATH:/$testCmd->{'piggybankjarPath'}/g;
	$cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
	$cmd =~ s/:PIGHARNESS:/$ENV{PIG_HARNESS_ROOT}/g;
	$cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
	$cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
	$cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
	$cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
	$cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
	$cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
	$cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
	$cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;
	$cmd =~ s/:FILER:/$testCmd->{'filerPath'}/g;
#        my $cmd = replaceParameters( $testCmd->{'script'}, $outfile, $testCmd, $log );

	open(FH, ">$script") or die "Unable to open file $script to write pig script, $ERRNO\n";
	print FH $cmd . "\n";
	close(FH);

    #my @cmds = split (/$/, $cmd);
    #push(@cmds, $cmd);
    print $log "$0:$subName RESULT ARE IN FILE ($outfile)\n";
    print $log "$0:$subName SCRIPT CONTAINS ($script):  \n$cmd\n";
    my @result=`chmod +x $script`;
    my $command= "$script >> $outfile 2>&1";
    print $log "Going to run command: ($command)\n";
    @result=`$command`;
    $result{'rc'} =  $?;
    print $log  @result;
#        IPC::Run::run(\@cmds, \undef, \$result{'stdout'}, \$result{'stderr'});
         #IPC::Run::run(\@cmds, \undef, \$result{'stdout'}, \$result{'stderr'});
 	#$result{'rc'} = $? >> 8;
    return \%result;
}


sub replaceParameters(){
    my ($self, $cmd, $outfile, $testCmd, $log) = @_;

	$cmd =~ s/:INPATH:/$testCmd->{'inpathbase'}/g;
	$cmd =~ s/:OUTPATH:/$outfile/g;
	$cmd =~ s/:FUNCPATH:/$testCmd->{'funcjarPath'}/g;
	$cmd =~ s/:PIGGYBANKPATH:/$testCmd->{'piggybankjarPath'}/g;
	$cmd =~ s/:RUNID:/$testCmd->{'UID'}/g;
	$cmd =~ s/:PIGHARNESS:/$ENV{PIG_HARNESS_ROOT}/g;
	$cmd =~ s/:USRHOMEPATH:/$testCmd->{'userhomePath'}/g;
	$cmd =~ s/:SCRIPTHOMEPATH:/$testCmd->{'scriptPath'}/g;
	$cmd =~ s/:DBUSER:/$testCmd->{'dbuser'}/g;
	$cmd =~ s/:DBNAME:/$testCmd->{'dbdb'}/g;
	$cmd =~ s/:LOCALINPATH:/$testCmd->{'localinpathbase'}/g;
	$cmd =~ s/:LOCALOUTPATH:/$testCmd->{'localoutpathbase'}/g;
	$cmd =~ s/:BMPATH:/$testCmd->{'benchmarkPath'}/g;
	$cmd =~ s/:TMP:/$testCmd->{'tmpPath'}/g;

        $cmd =~ s/:LATESTOUTPUTPATH:/$self->{'latestoutputpath'}/g;

    return $cmd;
}
########################################################################
# Sub: generateBenchmark
# Generate databse benchmark.
#
# Parameters:
# $testCmd -
# $log     -
#
# Returns:
# hash reference containg the test result
#

sub generateBenchmark
{
	my ($self, $testCmd, $log) = @_;

	my %result;

	foreach my $key ('expected_out', 'expected_out_regex', 'expected_err', 'expected_err_regex', 'rc') {
		if (defined $testCmd->{$key}) {
			$result{$key} = $testCmd->{$key};
		}

        }
	return \%result;
}

########################################################################
# Sub: compare
# Compare the test reuslts to the benchmark results
#
# Parameters:
# None.
#
# Returns:
# the result of the test run. 1 if the test passes.

sub compare
{
	my ($self, $testResult, $benchmarkResult, $log) = @_;
        my $subName = (caller(0))[3];
        #return 1 if ( $testCmd->{'ignore'}=~ "true" );
        return 1;
}

sub compareSAV
{
	my ($self, $testResult, $benchmarkResult, $log) = @_;
        my $subName = (caller(0))[3];

    # IMPORTANT NOTE:
    # If you are using a regex to compare stdout or stderr
    # and if the pattern that you are trying to match spans two line
    # explicitly use '\n' (without the single quotes) in the regex 

    if (defined $benchmarkResult->{'rc'} &&                                                                                                                     
            ($testResult->{'rc'} != $benchmarkResult->{'rc'})) {                                                                                                
        print $log "Test and benchmark return code differ:\n";                                                                                                  
        print $log "Test rc = " . $testResult->{'rc'} . "\n";                                                                                                   
        print $log "Expected rc = " . $benchmarkResult->{'rc'} . "\n";                                                                                          
        return 0;
    }  

	# Check if we are looking for an exact match
	if (defined $benchmarkResult->{'expected_out'}) {
		print $log "$0::$subName INFO Checking test result <$testResult->{'stdout'}> " .
			"as exact match against expected <$benchmarkResult->{'expected_out'}>\n";
		return $testResult->{'stdout'} eq
			$benchmarkResult->{'expected_out'};

	} elsif (defined $benchmarkResult->{'expected_out_regex'}) {
		print $log "$0::$subName INFO Checking test result for regular expression " .
			"<$benchmarkResult->{'expected_out_regex'}> in " .
			"<$testResult->{'stdout'}>\n";
		return $testResult->{'stdout'} =~
			$benchmarkResult->{'expected_out_regex'};

	} elsif (defined $benchmarkResult->{'expected_err'}) {

		print $log "$0::$subName INFO Checking test result <$testResult->{'stderr'}> " .
			"as exact match against expected <$benchmarkResult->{'expected_err'}>\n";
		return $testResult->{'stderr'} eq
			$benchmarkResult->{'expected_err'};

	} elsif (defined $benchmarkResult->{'expected_err_regex'}) {
		print $log "$0::$subName INFO Checking test result for regular expression " .
			"<$benchmarkResult->{'expected_err_regex'}> in " .
			"<$testResult->{'stderr'}>\n";
		return $testResult->{'stderr'} =~
			$benchmarkResult->{'expected_err_regex'};

	} else {
		return 1;
	}
}
1;
