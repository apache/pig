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
                                                                                       
package TestDriverTest;

use TestDriver;
use IPC::Run; # don't do qw(run), it screws up TestDriver which also has a run method

our @ISA = "TestDriver";

use strict;

sub new
{
	# Call our parent
	my ($proto) = @_;
	my $class = ref($proto) || $proto;
	my $self = $class->SUPER::new;

	bless($self, $class);

	return $self;
}

sub runTest
{
	my ($self, $testCmd, $log, $copyResults) = @_;

	my %result;

	$result{'rc'} = $testCmd->{'rc'};
	return \%result;
}

sub generateBenchmark
{
	my ($self, $testCmd, $log) = @_;

	my %result;
	$result{'rc'} = $testCmd->{'benchmark_rc'};
	return \%result;
}

sub compare
{
	my ($self, $testResult, $benchmarkResult, $log) = @_;

	return $testResult->{'rc'} == $benchmarkResult->{'rc'};
}
1;
