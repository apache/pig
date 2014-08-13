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
    outputSchemaFunction :squareSchema

    def square num
        return nil if num.nil?
        num**2
    end

    def squareSchema input
        input
    end


    outputSchemaFunction :squareSchema

    def redirect num
        return square(num)
    end

    outputSchema "word:chararray"

    def concat *input
        input.inject(:+)
    end

    def byteconcat *input
        concat *input
    end

    outputSchema "t:(m:[], t:(name:chararray, age:int, gpa:double), b:{t:(name:chararray, age:int, gpa:double)})"

    def complexTypes(m, t, b)
        outm = {}
        outm["name"] = m["name"].to_s.length if m

        outt = Array.new(3)
        outt = t.reverse if t

        outb = DataBag.new
        if b
            b.each do |x|
                tmpout = Array.new(3)
                tmpout = x.reverse
                outb.add(tmpout)
            end
        end
        out = Array.new(3)
        out[0] = outm if !outm.empty?
        out[1] = outt if !outt.all? {|x| x.nil?}
        out[2] = outb if outb.size() > 0
        out
    end

end

class Count < AlgebraicPigUdf
    output_schema Schema.long

    def initial t
        t.nil? ? 0 : 1
    end

    def intermed t
        return 0 if t.nil?
        t.flatten.inject(:+)
    end

    def final t
        intermed(t)
    end

end

class Sum < AccumulatorPigUdf
    output_schema { |i| i.in.in[0] }

    def exec items
        @sum ||= 0
        @sum += items.flatten.inject(:+)
    end

    def get
        @sum
    end
end
