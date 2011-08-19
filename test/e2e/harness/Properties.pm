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
# Class: Properties
#
# The Properties class represents a persistent set of properties. The Properties can # be saved to a file or loaded from file. Each key and its corresponding value in 
# the property list is a string. The properties are loaded as variable members of 
# the object hash.
#

use strict;
use English;
use Carp;

require Exporter;
@Properties::ISA=qw(Exporter);

package Properties;
unshift( @INC, ".");


##############################################################################
#  Sub: new
#  Properties Constructor. The constroctor sets the default logging levels as
#  follows:
#  - Looks for
# Returns:
# $self - object reference

# Parameters:
#    Debug       - 1 to enable debug statements, 0 otherwise
#    SrcFilename - The full path to the properties file.
# 
#


sub new() {
   my $class= shift;
   my $debug= shift;

   my $self= {
       Debug         => 0
      ,SrcFilename   => shift
   };

   bless( $self, $class );

   $self->{Debug}=1 if ( $debug );

   $self->readCfg();

   while (  $self->replaceVariables() ) {}

#   $self->print();
   $self->print() if ( $debug );

   return $self;
}


##############################################################################
# Sub: readCfg
# Private subroutine which reads the proerty file and stores properties as hash.
# Reads the configuration file and adds properties to Property object
# Process the keys and removes any spaces and makes them all lower case.
#
# Paramaters:
# None
#

sub readCfg($)
{
   my $self = shift;
   my $subName  = (caller(0))[3];
   my $cfgFile = $self->{SrcFilename};

   open ( CFG, $self->{SrcFilename} )
     || Carp::confess ( "ERROR $0.$subName at ".__LINE__.": can't open " . $self->{SrcFilename} . "\n" );

   my $cfgContents;

    while (<CFG>){
       next if ( $_ =~ '#' );
      $cfgContents .= $_ ;
   }

   close CFG;

   my $cfg = undef;
   eval("$cfgContents");

   if ($@) {
      chomp $@;
      die "FATAL ERROR $0.$subName at ".__LINE__." : Error reading config file <$cfgFile>, <$@>\n";
   }

   if (not defined $cfg) {
           die "FATAL ERROR $0 at ".__LINE__." : Configuration file <$cfgFile> should have defined \$cfg\n";
   }

   if ( ref( $cfg ) !~ "HASH" ){
           die "FATAL ERROR $0 at ".__LINE__." : Configuration file <$cfgFile> should return a hash\n";
  }

   # Add the name of the file
   $cfg->{'file'} = $cfgFile;

   my @keys = keys( %$cfg );

   # Add keys to this object
   # removes spaces from key and sets key to lower case
   foreach my $key ( @keys ) {
 
         $key =~ s/\s*//g;
   #      $key= lc($key);
         $self->{$key} = $$cfg{$key};
 
    }

}

##############################################################################
#  Sub: replaceVariables
#  Finds variables in properties and replaces the varialbes with the actual value.
#  Using the objects hash, searches for  all _values_ in a key=value pair that 
#  qualify as a variable. A variable is identified as having a e pattern "${key}".
#  When a variable is found, it is looked up in the hash, and replaed with it's
#  value.
#  
#  For example, if the properties file has mypath=${root.path}/mypath, ${root.path}
#  will be replaced with the actual value  mypath=/usr/foo/mypath
#

sub replaceVariables() {
   my $self    = shift;
   my $subName  = (caller(0))[3];
   my @keys    = sort keys %$self;

   my @list;
   my @strings;
   my @variables;
   my $value;
   my $count=0;

   for my $key ( @keys ) {

      $value=$self->{$key};
       for my $variable ( @keys ) {
         next if ( $value !~ /\{$variable\}/ );
         $count++;
         $value =~ s/\$\{$variable\}/$self->{$variable}/g;
         $self->{$key}=$value;

       }

   }

  return $count;
}

##############################################################################
#  Sub: init
#  Public subroutine which returns the name of the properties file.
# 
# Paramaters:
# None
sub getFilename() {
   my $self = shift;
   my $subName  = (caller(0))[3];
   return $self->{SrcFilename};
}


##############################################################################
#  Sub: init
#  Public subroutine which returns the value of a property key.
# 
# Paramaters:
# $key = the property to look up
#
# Returns:
# $value - the value of the property
#
sub getProperty() {
   my $self    = shift;
   my $subName  = (caller(0))[3];
   my $key     = shift;

   return $self->{$key};
}

##############################################################################
#  Sub: getAndVerifyProperty
#  Public subroutine which returns the value of a property key and dies
#  if the property does not exist.
# 
# Paramaters:
# $key = the property to look up
#
# Returns:
# $value - the value of the property
#
sub getAndVerifyProperty() {
   my $self    = shift;
   my $subName  = (caller(0))[3];
   my $key     = shift;

   if ( ! $self->{$key} ) {
       die "FATAL ERROR: " . ref($self) . "$subName(): either the property $key odes not exist or it is set to null\n";
   }
   return $self->{$key};
}

##############################################################################
#  Sub: getAndVerifyProperty
#  Public subroutine that prints the properties to stdout.
# 
# Paramaters:
# None
#
# Returns:
# None 
#
sub print(){
   my $self    = shift;
   my $subName  = (caller(0))[3];
   my @keys    = sort keys %$self;

   my @list;
   foreach my $key ( @keys ) {
     print "$key = ". $self->{$key} . "\n";
   }
  return 1;
}

##############################################################################
# Sub: getPropertyValuesByFilter
#  Public subroutine that finds all properties whose key matches the pattern.
# 
# Paramaters:
# $pattern = the pattern to match
#
# Returns:
# $ref - A reference to a list of property values found.


sub getPropertyValuesByFilter(){
   my $self    = shift;
   my $subName  = (caller(0))[3];
   my $pattern = shift;
   my @keys    = sort keys %$self;

   my @list;
   foreach my $key ( @keys ) {
     push (@list,$self->{$key}) if ( $key =~ "$pattern" );
   }

   return \@list;
}

1;
#use Properties;
#
#sub main() {
#
#my $properties= new Properties( 1, "$ENV{'HARNESS_ROOT'}/conf/test.conf" );
#}

#&main();
