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
use English;


# A very simple script to check if two explain plans match.  The script
# will simply do a line to line check.  The expected can contain regular
# expressions.

my $actual = $ARGV[0];
my $expected = $ARGV[1];

if (!(defined $actual) || !(defined $expected)) {
    die "Usage: $0 actual expected\n";
}

open(F1, "<$actual") or die "$0::explainchk at ".__LINE__.": Cannot open $actual for reading, $!\n";
open(F2, "<$expected") or die "$0::explainchk at ".__LINE__.": Cannot open $actual for reading, $!\n"; 

my $errcnt = 0;

for (my $i = 0; my $actualLine = <F1>; $i++) {
    chomp $actualLine;
    my $expectedLine = <F2>;
    if (! defined $expectedLine) {
        $errcnt++;
        last;
    }
    chomp $expectedLine;

    next if ($actualLine =~ /$expectedLine/);

    warn "$0: Difference found at line $i, actual has <$actualLine>, expected has /$expectedLine/\n";
    $errcnt++;
}

exit $errcnt;
