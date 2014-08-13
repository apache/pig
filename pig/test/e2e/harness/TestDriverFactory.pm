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
                                                                                       
package TestDriverFactory;

###############################################################################
# Class: TestDriverFactory
# A factory for TestDrivers.
#
 
use strict;

# Export utility functions so I don't have to type TestDriverFactory:: over
# and over.
require Exporter;
our @ISA = "Exporter";
our @EXPORT_OK = qw(splitLine readLine isTag);

###############################################################################
# Sub: getTestDriver
# Returns the appropriate Test Driver.
#
# Returns:
# instance of appropriate subclass of TestDriver
#
sub getTestDriver
{
	my $cfg = shift;

	if (not defined $cfg->{'driver'}) {
		die "$0 FATAL : I didn't see a driver key in the file, I don't know what driver "
			. "to instantiate.\n";
	}

 
	my $className = "TestDriver" . $cfg->{'driver'};
    require "$className.pm";
	my $driver = new $className();

	return $driver;
}

1;
