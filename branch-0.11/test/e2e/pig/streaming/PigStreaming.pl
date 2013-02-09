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

# This script is used to test streaming in pig.
# Usage: PigStreaming.pl <input> <output> <support file>
# Without any parameters it would read data from stdin and write the results to stdout
# Otherwise it would read/write to specified input/output file; - can be used to specify stdin and stdout
# If support file is specified, it is loaded into a hash and input data is used as the key and matched
# values are outputed; otherwise the read data is written out

my $input = "-";
my $output = "-";
my %hash = undef;
my $input_handle;
my $output_handle;

if ($#ARGV >= 0)
{
	$input = $ARGV[0];
	if ($#ARGV >= 1)
	{
		$output = $ARGV[1];
		if ($#ARGV >= 2)
		{
			loadHash($ARGV[2]);
		}
	}		

}

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

print STDERR "PigStreaming.pl: starting processing\n";

my $data;
while (<$input_handle>)
{
	chomp;	
	$data = $_;
	if (defined(%hash) && (exists $hash{$data}))
	{
		print $output_handle "$hash{$data}\n";		
	}
	else
	{
		print $output_handle "$data\n";
	}
}

close $input_handle;
close $output_handle;

print STDERR "PigStreaming.pl: Done\n";

exit 0;

sub loadHash($)
{
	my $lookupFile = shift;
	open(LOOKUP, $lookupFile) || die "Can't open $lookupFile\n";
	while (<LOOKUP>)
	{
		chomp;
		my @row = split(/\t/, $_);
		$hash{$row[0]} = $row[1];
	}

	close LOOKUP;
}

