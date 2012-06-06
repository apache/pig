#!/usr/bin/python

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
                                                                                       
@outputSchemaFunction("squareSchema")
def square(num):
    return ((num)*(num))

@schemaFunction("squareSchema")
def squareSchema(input):
    return input

@outputSchema("word:chararray")
def concat(word):
    return word + word

@outputSchema("gpa:double")
def adjustgpa(gpa, instate):
    if instate == None:
        return None
    elif instate:
        return gpa
    else:
        return gpa+1

@outputSchema("retired:boolean")
def isretired(age):
    if age == None:
        return None
    elif age>=60:
        return True
    else:
        return False

outputSchema("words:{(word:chararray)}")
def tokenize(sentence):
    return sentence.split(' ')
