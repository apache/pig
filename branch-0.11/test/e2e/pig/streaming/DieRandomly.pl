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
                                                                                       
###########################################################################
# Package: DieRandomply
# Script which terminates after consuming random amount of input lines
# Writes no output to stdout
# 


#####################################################################################
#
#         FILE:  DieRandomly.pl
#
#        USAGE:  ./DieRandomly.pl 
#
#  DESCRIPTION:  Script which terminates after consuming random amount of input lines
#                Writes no output to stdout
#
#      OPTIONS:  ---
# REQUIREMENTS:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:   (), <>
#      COMPANY:  
#      VERSION:  1.0
#      CREATED:  08/28/2008 03:55:37 PM PDT
#     REVISION:  ---
#####################################################################################

use strict;
use warnings;


if(scalar(@ARGV) != 2) {
    die "Usage: $0 <number of lines in input> <exit code>";
}

my $numInputLines = shift;
my $exitCode = shift;

my $terminateAt = int(rand($numInputLines));
my $i = 0;
while(<STDIN>) {
   $i++;
   if($i == $terminateAt) {
       exit($exitCode); 
   }
}
