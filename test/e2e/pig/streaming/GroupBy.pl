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

###########################################################################
# Package: GroupBy
#
# This script is used to test streaming in pig.  It allows to compute count(*) 
# based on the group key specified as column positions within data
# Usage: GroupBy.pl <delimiter> <list of group by columns>
# Example: GroupBy.pl '\t' 1 3


if ($#ARGV < 0)
{
	print "Usage: GroupBy.pl <delimiter> <list of group by columns>\nExample: GroupBy.pl '\t' 1 3\n";
	exit(1);
}

my $delim = $ARGV[0];
my $i;
my @pos;

for ($i = 0; $i < $#ARGV; $i++)
{
	$pos[$i] = $ARGV[$i+1];
}

my $key = undef;
my $count = 0;
my $new_key;
while (<STDIN>)
{
	chomp;	
	my @row = split(/$delim/, $_);
	$new_key = "";
	for ($i = 0; $i <= $#pos; $i++)
	{
		$new_key = "$new_key$row[$pos[$i]]\t";
	}

	if ($new_key eq $key)
	{
		$count ++;
	}
	else
	{
		if (defined($key))
		{
			print "$key$count\n";
		}
		$key = $new_key;
		$count = 1;
	}
}

if (defined($key))
{
	print "$key$count\n";
}

exit 0;
