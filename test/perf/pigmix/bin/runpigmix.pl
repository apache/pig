#!/usr/local/bin/perl -w

if(scalar(@ARGV) < 6 )
{
    print STDERR "Usage: $0 <pig_home> <pig_bin> <pigmix_jar> <hadoop_home> <hadoop_bin> <pig mix scripts dir> <hdfs_root> <pigmix_output> [parallel] [numruns] [runmapreduce] \n";
    exit(-1);
}
my $pighome = shift;
my $pigbin = shift;
my $pigmixjar = shift;
my $hadoophome = shift;
my $hadoopbin = shift;
my $scriptdir = shift;
my $hdfsroot = shift;
my $pigmixoutput = shift;
my $parallel = shift;
my $runs = shift;
my $runmapreduce = shift;
my $pigjar = "$pighome/pig-withouthadoop.jar";
if(!defined($parallel)) {
    $parallel = 40;
}
if(!defined($runs)) {
    $runs = 3;
}
if(!defined($runmapreduce)) {
    $runmapreduce = 1;
}

$ENV{'HADOOP_HOME'} = $hadoophome;
$ENV{'HADOOP_CLIENT_OPTS'}="-Xmx1024m";

my $cmd;
my $total_pig_times = 0;
my $total_mr_times = 0;

print STDERR "Removing output dir $pigmixoutput \n";
$cmd = "$hadoopbin fs -rmr $pigmixoutput";
print STDERR "Going to run $cmd\n";
print STDERR `$cmd 2>&1`;

for(my $i = 1; $i <= 17; $i++) {
    my $pig_times = 0;
    for(my $j = 0; $j < $runs; $j++) {
        print STDERR "Running Pig Query L".$i."\n";
        print STDERR "L".$i.":";
        print STDERR "Going to run $pigbin $scriptdir/L".$i.".pig\n";
        my $s = time();
        $cmd = "$pigbin -param PIGMIX_JAR=$pigmixjar -param HDFS_ROOT=$hdfsroot -param PIGMIX_OUTPUT=$pigmixoutput/pig -param PARALLEL=$parallel $scriptdir/L". $i.".pig" ;
        print STDERR `$cmd 2>&1`;
        my $e = time();
        $pig_times += $e - $s;
        cleanup($i);
    }
    # find avg
    $pig_times = $pig_times/$runs;
    # round to next second
    $pig_times = int($pig_times + 0.5);
    $total_pig_times = $total_pig_times + $pig_times;

    if ($runmapreduce==0) {
        print "PigMix_$i pig run time: $pig_times\n";
    }
    else {
        $mr_times = 0;
        for(my $j = 0; $j < $runs; $j++) {
            print STDERR "Running Map-Reduce Query L".$i."\n";
            print STDERR "L".$i.":";
            print STDERR "Going to run $hadoopbin jar $pigmixjar org.apache.pig.test.pigmix.mapreduce.L"."$i $hdfsroot $pigmixoutput/mapreduce $parallel\n";
            my $s = time();
            $cmd = "$hadoopbin jar $pigmixjar org.apache.pig.test.pigmix.mapreduce.L$i $hdfsroot $pigmixoutput/mapreduce $parallel";
            print STDERR `$cmd 2>&1`;
            my $e = time();
            $mr_times += $e - $s;
            cleanup($i);
        }
        # find avg
        $mr_times = $mr_times/$runs;
        # round to next second
        $mr_times = int($mr_times + 0.5);
        $total_mr_times = $total_mr_times + $mr_times;


	my $multiplier=0;
	if ($mr_times!=0) 
	    $multiplier = $pig_times/$mr_times;
        print "PigMix_$i pig run time: $pig_times, java run time: $mr_times, multiplier: $multiplier\n";
    }
}

if ($runmapreduce==0) {
    print "Total pig run time: $total_pig_times\n";
}
else {
    my $total_multiplier = $total_pig_times / $total_mr_times;
    print "Total pig run time: $total_pig_times, total java time: $total_mr_times, multiplier: $total_multiplier\n";
}

sub cleanup {
    my $suffix = shift;
    my $cmd;
    $cmd = "$pigbin -e rmf L".$suffix."out";
    print STDERR `$cmd 2>&1`;
    $cmd = "$pigbin -e rmf highest_value_page_per_user";
    print STDERR `$cmd 2>&1`;
    $cmd = "$pigbin -e rmf total_timespent_per_term";
    print STDERR `$cmd 2>&1`;
    $cmd = "$pigbin -e rmf queries_per_action";
    print STDERR `$cmd 2>&1`;
    $cmd = "$pigbin -e rmf tmp";
    print STDERR `$cmd 2>&1`;
}

