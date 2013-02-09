#!/usr/local/bin/perl
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use File::Basename;
use Getopt::Long;
if (@ARGV > 0 ){
  GetOptions( \%options,
  'cmd:s',
  'smoke:s',
  'help:i'
  );
}
if (defined $options{'help'} or !defined $options{'smoke'}){
  print ("perl run.pl --smoke\n");
  print ("***************Enviroment Variables Needed***************\n");
  print ("HADOOP_HOME\n");
  print ("USER\n");
  print ("ZEBRA_JAR\n");
  print ("PIG_JAR\n");
  print ("ZEBRA_SMOKE_JUNIT_JAR\n");
  print ("CLASSPATH\n");
  print ("HADOOP_CLASSPATH\n");
  print ("ZEBRA_SMOKE_PIG_CLASS\n");
  print ("ZEBRA_SMOKE_MAPRED_CLASS\n");
  exit;
}

#make an output directory
$zebraqa= $ENV{ZEBRA_SMOKE_DIR};
mkdir("$zebraqa/output");
$logfile="$zebraqa/output/zebra.out";

if (defined $options{'smoke'}){
  $my_hadoop_home=$ENV{HADOOP_HOME};
  defined($my_hadoop_home) || die("HADOOP_HOME not defined");
  $my_user=$ENV{USER};
  defined($my_user) || die("USER not defined");
  $my_zebra_jar=$ENV{ZEBRA_JAR};
  defined($my_zebra_jar) || die("ZEBRA_JAR not defined");
  $my_pig_jar=$ENV{PIG_JAR};
  defined($my_pig_jar) || die("PIG_JAR not defined");
  $my_junit_jar=$ENV{ZEBRA_SMOKE_JUNIT_JAR};
  defined($my_junit_jar) || die("ZEBRA_SMOKE_JUNIT_JAR not defined");
  $my_classpath=$ENV{CLASSPATH};
  defined($my_classpath) || die("CLASSPATH not defined");
  $my_smoke_pig_class=$ENV{ZEBRA_SMOKE_PIG_CLASS};
  defined($my_smoke_pig_class) || die("ZEBRA_SMOKE_PIG_CLASS not defined");
  $my_mapred_class=$ENV{ZEBRA_SMOKE_MAPRED_CLASS};
  defined($my_mapred_class) || die("ZEBRA_SMOKE_MAPRED_CLASS not defined");
  $my_hadoop_classpath=$ENV{HADOOP_CLASSPATH};
  defined($my_hadoop_classpath) || die("HADOOP_CLASSPATH not defined");
	
  #execute pig job
  write_to_log("..... STARTING ZEBRA PIG JOB TESTING .....\n");
  $cmd="java -cp $my_classpath -DwhichCluster=\"realCluster\" -DHADOOP_HOME=$my_hadoop_home -DUSER=$my_user org.junit.runner.JUnitCore $my_smoke_pig_class > $zebraqa/output/zebra.out 2>&1";
  exec_cmd($cmd);

  #execute mapred job
  write_to_log("..... STARTING ZEBRA MAP REDUCE JOB TESTING .....\n");	
  $cmd="$my_hadoop_home/bin/hadoop jar $zebraqa/lib/zebra_smoke.jar $my_mapred_class -libjars $my_pig_jar,$my_zebra_jar,$my_junit_jar >> $zebraqa/output/zebra.out 2>&1";
  exec_cmd($cmd);
}

sub exec_cmd {
  my $cmd = shift or die "exec_cmd: Command not supplied!\n";
  print ($cmd."\n");
  my $rc = system($cmd);
  !$rc or die "ERROR($rc): command failed: $cmd\n";
  return $rc;
}
sub write_to_log {
  ($whatToWrite,$ignore) = @_;
  open(MYLOG, ">>$logfile") or die "can't open $logfile : $!\n";
  print MYLOG $whatToWrite."\n";
  close MYLOG;
}


