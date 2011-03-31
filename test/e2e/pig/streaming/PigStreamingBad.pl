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
                                                                                       
use strict;

# This script is used to test streaming error cases in pig.
# Usage: PigStreaming.pl <start|middle|end> 
# the parameter tells the application when to exit with error

if ($#ARGV < 0)
{
	print STDERR "Usage PigStreaming.pl <start|middle|end>\n";
	exit (-1);
}

my $pos = $ARGV[0];

if ($pos eq "start")
{
	print STDERR "Failed in the beginning of the processing\n";
	exit(1);
}


print STDERR "PigStreamingBad.pl: starting processing\n";

my $cnt = 0;
while (<STDIN>)
{
	print "$_";
	$cnt++;
	print STDERR "PigStreaming.pl: processing $_\n";
	if (($cnt > 100) && ($pos eq "middle"))
	{
		print STDERR "Failed in the middle of processing\n";
		exit(2);
	} 
}

print STDERR "Failed at the end of processing\n";
exit(3);

