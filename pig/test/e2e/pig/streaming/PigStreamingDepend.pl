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
use PigStreamingModule;

# This script is used to test dependency and custom serializer in pig
# Usage: PigStreaming.pl <input> <output> 
# Without any parameters it would read data from stdin and write the results to stdout
# Otherwise it would read/write to specified input/output file; - can be used to specify stdin and stdout

my $input = "-";
my $output = "-";
my $second_output = undef;

if ($#ARGV >= 0)
{
	$input = $ARGV[0];
	if ($#ARGV >= 1)
	{
		$output = $ARGV[1];
		if ($#ARGV >= 1)
		{
			$second_output = $ARGV[2];
		}
	}		
}

print STDERR "PigStreamingDep.pl: starting processing\n";
PigStreamingModule::transformData($input, $output, $second_output);
print STDERR "PigStreamingDep.pl: done\n";

exit 0;
