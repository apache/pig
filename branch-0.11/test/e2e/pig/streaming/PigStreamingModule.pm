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
                                                                                       
package PigStreamingModule;

use strict;

# This module parsers data in the format produced by PigDump
# and write it tab separated
# input format:
# (field1, field2,...)

sub transformData($$;$)
{
	my $input = shift;
	my $output = shift;
	my $second_output = shift;
	my $input_handle;
	my $output_handle;
	my $second_output_handle = undef;

	if ($input eq "-")
	{
		$input_handle = \*STDIN;
	}
	else
	{
		open(INPUT, $input) || die "Can't open $input\n";
		$input_handle = \*INPUT;
	}

	if ($output eq "-")
	{
		$output_handle = \*STDOUT;
	}
	else
	{
		open(OUTPUT, ">$output") || die "Can't create $output\n";
		$output_handle = \*OUTPUT;
	}

	if (defined($second_output))
	{
		open(SECOND_OUTPUT, ">$second_output") || die "Can't create $second_output\n";
		$second_output_handle = \*SECOND_OUTPUT;
	}

	while (<$input_handle>)
	{
		chomp;	
				
                print $output_handle $_;
		if (defined($second_output_handle))
		{
		    print $second_output_handle $_;			    
		}

		if (defined($second_output_handle))
		{
			print $second_output_handle "\n";
		}
		print $output_handle "\n";
	}

	close $input_handle;
	close $output_handle;
	if (defined($second_output_handle))
	{
		close $second_output_handle;
	}
}

1;

