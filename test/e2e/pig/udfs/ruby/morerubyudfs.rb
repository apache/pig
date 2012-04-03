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
#
############################################################################
require 'pigudf'

class Myudfs < PigUdf
    def square num
        return num**2 if num
    end

    def cube num
        return num**3 if num
    end

    output_schema_function :rev_schema

    def reverse e1, e2
      [e2,e1]
    end

    def rev_schema s
      Schema.t([s[1],s[0]])
    end
end

CUBE = PigUdf.evalfunc("x:int") do |v|
    v**3
end

ISEVEN = PigUdf.filterfunc do |v|
    v % 2 == 0
end

class AppendIndex < AccumulatorPigUdf
    outputSchema do |inS|
        inS.in.in.add! Schema.long
        inS
    end

    def initialize
      @ct = 0
      @inter = DataBag.new
    end

    def exec b
        b.each { |t| @inter.add(t << (@ct+=1)) }
    end

    def get
        @inter
    end
end
