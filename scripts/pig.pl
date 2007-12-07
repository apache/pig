#!/usr/local/bin/perl

use strict;
use File::Find;
use English;

sub processClasspath();
sub getJar($);
sub getConfigDir();

# Process args
our $latest = 0;
our $cluster = undef;
our $classpath =  undef;
our @javaArgs;
our $debug = 0;
our $hodParam = "";

# Read our configuration file.  This will fill in values for pigJarRoot
# and hodRoot.
our $ROOT = (defined($ENV{'ROOT'}) ? $ENV{'ROOT'} : "/home/y");
my ($pigJarRoot, $hodRoot, $defaultCluster);

open(CFG, "< $ROOT/conf/pigclient.conf") or
	die "Can't open $ROOT/conf/pigclient.conf, $ERRNO\n";

my $cfgContents;
$cfgContents .= $_ while (<CFG>);
close(CFG);

eval("$cfgContents");

for (my $i = 0; $i < @ARGV; ) {
	if ($ARGV[$i] eq "-cp" || $ARGV[$i] eq "-classpath") {
		$classpath = $ARGV[$i + 1];
		splice(@ARGV, $i, 2);
	} elsif ($ARGV[$i] eq "-c" || $ARGV[$i] =~ /--?cluster/) {
		$cluster = $ARGV[$i + 1];
		splice(@ARGV, $i, 2);
	} elsif ($ARGV[$i] =~ /-D/) {
		my $val = splice(@ARGV, $i, 1);
		if ($val =~/-Dhod\.param=(.*)/){
			$hodParam .="$1 "; 
		}else{
			push(@javaArgs, $val);
		}
	} elsif ($ARGV[$i] eq "-l" || $ARGV[$i] =~ /--?latest/) {
		$latest = 1;
		splice(@ARGV, $i, 1);
	} elsif ($ARGV[$i] eq "-secretDebugCmd") {
		$debug = 1;
		splice(@ARGV, $i, 1);
	} elsif ($ARGV[$i] eq "-h" || $ARGV[$i] eq "--help") {
		print STDERR "pig script usage:  pig\n";
		print STDERR "\t-l, --latest\n";
		print STDERR "\t\tuse latest, untested, unsupported version of pig.jar instaed of relased, tested, supported version.\n";
		print STDERR "\t-c, --cluster _clustername_\n";
		print STDERR "\t\trun on cluster _clustername_ instead of default kryptonite.\n";
		print STDERR "pig.jar help:\n";
		$i++;
	} else {
		$i++;
	}
}

processClasspath();

if (defined $classpath)
{
    # Check to make sure that the jar file specified in the class path is
    # available.
    $classpath =~ /(^|:)([^:]*pig.jar)($|:)/;
    my $jar = $2;
    if (!(-e $jar)) {
        die "I can't find the jar file $jar.  If you explicitly
put this jar in your classpath, please check that you have the path name
correct.  If you specified a cluster via -c[luster], then the pig jar for
that cluster is not present on this machine.\n";
    }
	push (@javaArgs, "-cp", $classpath);
}

# Make sure we can find java
my $java = 'java';
`which java 2>&1 > /dev/null`;
if ($? != 0) {
	# Couldn't find it in the path, let's try to find it in the standard
	# location.
	if (-e "/usr/releng/tools/java/current/bin/java") {
		$java = '/usr/releng/tools/java/current/bin/java';
	} else {
		die "I can't find java, please include it in your PATH.\n";
	}
}

# Add any java args (-cp, -D) they gave us
my @cmd = ("$java");
push(@cmd, @javaArgs); 

# If we aren't attaching to kryptonite, set up the right hod config file.
if ($cluster ne "kryptonite") {
    # With splitting of gateways, HOD file is always hodrc, no matter what
    # cluster you're talking to.
	my $hodCfg = "$hodRoot/conf/hodrc";
	if (! (-e $hodCfg)) {
		push(@cmd, "-Dhod.server=");
		warn "I can't find HOD configuration for $cluster, hopefully you weren't planning on using HOD.\n";
	}
}

if ($hodParam ne "")
{
	push(@cmd, "-Dhod.param=$hodParam");
}

# Add our jar to the command
push(@cmd, "org.apache.pig.Main");

# Add any remaining args to the command
push(@cmd, @ARGV);

# Run 
if ($debug) { die "Would run " . join(" ", @cmd) . "\n"; }
exec @cmd;

sub processClasspath()
{
	# first, figure out if we are working with a deployed cluster 
	if (!(defined $cluster) && (!(defined $classpath) || !($classpath =~/pig.jar/)))
	{
		# we are using default cluster, the name of which is stored in the
        # pigclient.conf file.
		$cluster = $defaultCluster;
	}

	# we are running from a cluster
	# add pig.jar and hadoop-site.xml
	# having pig.jar in classpath is an error
	# having hadoop-site.xml is ok and its value would be used
	if (defined $cluster)
	{
		if (defined($classpath) && ($classpath =~/pig.jar/))
		{
			die "You requested to run on $cluster cluster. You can't overwrite pig.jar in that case. Please, remove it from your classpath.\n";
		}
		else
		{
			if (!defined($classpath)) 
			{
				$classpath = "";
			}
			else
			{
				$classpath .= ":";
			}
			$classpath .= getJar("pig.jar") .":" . getConfigDir(); 
			
		}
	}
}

# constructs path to jar file
# @param jar file
# @return full path of the jar file
sub getJar ($)
{
	my $jar = shift;
	my $location = "$pigJarRoot/libexec/pig/$cluster/";
	if ($latest) { $location .= "latest/"; }
	else { $location .= "released/"; }
	return "$location$jar"; 
}

# constructs path to cluster config dir
sub getConfigDir()
{
	my $location = "$pigJarRoot/conf/pig/$cluster/";
	if ($latest) { $location .= "latest/"; }
	else { $location .= "released/"; }
	return $location; 
}
