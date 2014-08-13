# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
throw "pigudf.rb only works under JRuby!" unless RUBY_PLATFORM=="java"
require 'jruby'
org.apache.pig.scripting.jruby.PigJrubyLibrary.new.load(JRuby.runtime, false)

#TODO output_schema should accept a Schema object as well, and use Schema objects
#TODO AccumulatorEvalFunc output_schema should not allow you to give a block and defined it

# This is the base class for runy of the mill EvalFuncs. A class just serves to
# contain similar jobs, as well as allow for method reuse. In the case of simple
# EvalFuncs, each method will be turned into a UDF (though they do not have to be called).
#
# TODO: EXPLAIN SYNTAX

class PigUdf

  # Here we initialize the variables we'll be using at the class level (generally
  # analogous to static in Java). The nice thing about this method is that these
  # values are all set in the same way, even from children. Thus, all of the children
  # will update PigUdf.@@functions_to_register, and on the Java side when we want to
  # access this, we can return that Map. This means it is no longer necessary to keep
  # track of descendent children, etc, since all that matters are the methods that
  # are registered with subclasses.
  #
  # The @@class_object_to_name_and_add variable is used by self.evalfunc and self.filterfunc.
  # See the documentation for the former to understand why it is necessary. @@schema holds
  # the last schema given by output_schema or output_schema_function, see the documentation
  # on output_schema for more.

  @@functions_to_register = {}
  @@class_object_to_name_and_add = nil
  @@schema = nil

  # See the documentation on self.evalfunc for why this is necessary. This takes the current class
  # object and registers it. This is necessary because self.evalfunc has to return before .to_s
  # will return something meaningful and not gibberish.

  def self.name_and_add_class_object
    if @@class_object_to_name_and_add
      name = @@class_object_to_name_and_add.class_object.to_s
      @@class_object_to_name_and_add.method_name = "eval"
      @@functions_to_register[name] = @@class_object_to_name_and_add
    end
    @@class_object_to_name_and_add = nil
  end

  # This is the core function that registers a method as a UDF. The pig_func_name
  # identifies it, and in most cases, is the method name (the exception begin
  # UDFs created using self.evalfunc). The class_object is the class against an instance
  # of which the method will be called. The arity is so Pig knows how many arguments
  # to pass to the UDF, and the output_schema defines the Schema of the output, either
  # as a string, or as a function.

  def self.register_function pig_func_name, class_object, arity, output_schema
    self.name_and_add_class_object

    pig_func_name = pig_func_name.to_s

    reg = EvalFunc.new class_object, pig_func_name, arity, output_schema

    @@functions_to_register[pig_func_name] = reg
  end

  def self.set_class_object_to_name_and_add func
    self.name_and_add_class_object
    @@class_object_to_name_and_add = func
  end

  # This method provides the most succinct way to define a UDF. The syntax is as follows:
  #
  # UdfName = PigUdf.evalfunc('int') do |arg1|
  #   return arg.length
  # end
  #
  # EvalFunc takes one parameter, the schema to be returned, and a block which will represent
  # the method call.
  #
  # In the case that this will be used, then it will be one class with one function,
  # and the function name will be UdfName. It is essential that UdfName begin with
  # a capital letter, as this method uses a hook given to ruby where Name = Class.new
  # will generate a class of name Name, but only if Name begins with a capital letter.
  #
  # The reason for naming the function "GETCLASSFROMOBJECT" is that the class object must first
  # be returned for its name to be available. Asking it for its name before allowing "evalfunc"
  # to return will not yield the name it is given. Thus, we plant "GETCLASSFROMOBJECT" so the next
  # time we access @functions_to_register, we know to check.

  def self.evalfunc output_schema, &blk
    c=Class.new do
      define_method :eval do |*args|
        blk.call(*args)
      end
    end
    self.set_class_object_to_name_and_add EvalFunc.new c, "GETFROMCLASSOBJECT", blk.arity, output_schema
    c
  end

  # This method functions identically to evalfunc above, the only difference being that no schema
  # needs to be given.

  def self.filterfunc &blk
    c=Class.new do
      define_method :eval do |*args|
        blk.call(*args)
      end
    end
    self.set_class_object_to_name_and_add EvalFunc.new c, "GETFROMCLASSOBJECT", blk.arity, Schema.boolean
    c
  end

  # This is the function which register the schema associated with a given function. There are
  # two ways that it can be invoked, with one argument or two (thus the vague argument names).
  #
  # case 1: one argument
  # In this case, output_schema's argument is the schema to be set for the next method declaration.
  # For example:
  # output_schema "long"
  #
  # The above would mean that the schema for the function following it would be set to long. The mechanism
  # by which this is achieved is by setting a class schema variable to the schema, and the next time
  # a method is declared in the class, the class uses the schema that was set to register the function being
  # declared. For more information on that, see self.method_added, as this is the Ruby provided hook
  # that is used to allow this disconnect between declaring a schema and the method declaration that follows.
  #
  # case 2: two arguments
  # In this case, arg1 is the name of the function whose schema we want to set, and arg2 is
  # the schema, ie
  #
  # output_schema :sum, "long"
  #
  # You can only use this after the function is declared, otherwise there will be an error.
  # In this case, the information passed to the registration function is the function name,
  # an instance of the class (so that on the Java side we can instantiate a version), the arity,
  # and the schema. For more information on how that information is used, see self.register_function.
  #
  # The following two uses are identical:
  #
  # use 1:
  # output_schema "long"
  # def sum x, y
  #   return x + y
  # end
  #
  # use 2:
  # def sum x,y
  #   return x + y
  # end
  # output_schema :sum, "long"

  def self.output_schema arg1, arg2=nil
    if arg2
      function_name = arg1.to_s
      schema = arg2.to_s
      self.register_function function_name, self, function_name, schema
    else
      @@schema = arg1
    end
  end

  # This function acts identically to output_schema, except that it is not necessary to provide a schema string
  # because a filter func will always have a set schema (it will return boolean).

  def self.filter_func arg1=nil
    schema = "FILTERFUNC"
    if arg1
      function_name = arg1.to_s
      self.output_schema function_name, schema
    else
      self.output_schema schema
    end
  end

  # output_schema is only useful when the function at hand has a deterministic schema. In the case that the schema
  # needs to be dynamic, it is useful to be able to process the input schema with a function and return the appropriate
  # output schema. An example of this might be a concat function, which takes two values and concatenates them together.
  # This function could work for chararrays, but also for bytearrays. In that case, the output schema depends on the input schema.
  #
  # As with output_schema, there are two cases, and they are identical (see output_schema for a more detailed explanation).
  # The difference, however, is that instead of passing a string ie "long", the user gives a function name. Note: the schema
  # function does not yet have to be defined. In the case of two arguments, the same information is passed to register_function
  # as in the case of output_schema, the difference being that while the schema is passed as a string, it has an identifier
  # appended to it so that when this function is running in Java, we'll know that we should be using a function.

  def self.output_schema_function arg1, arg2=nil #TODO allow it to also accept a block, as in ComplexPigUdf
    schema_func = (arg2||arg1).to_sym
    if arg2
      function_name = arg1.to_s
      self.register_function function_name, self, function_name, schema_func.to_sym
    else
      @@schema = arg1.to_sym
    end
  end

  # Javaists love their camelCase
  class << self
    alias :outputSchema :output_schema
    alias :filterFunc :filter_func
    alias :outputSchemaFunction :output_schema_function
  end

  # This is a hook that Ruby provides that is called whenever a method is declared on the subclass.
  # This is used so that we have visibility on the methods as they are declared, which is useful because
  # every declared method will be registered as a UDF for use in Pig. In the case of a method that doesn't
  # yet have a schema declared, it's return type will just be a bytearray, as in Pig.

  def self.method_added function_name
    if @@schema
      self.register_function function_name, self, function_name, @@schema
    elsif !@@functions_to_register[function_name]
      self.register_function function_name, self, function_name, nil
    end
    @@schema = nil
  end

  # This returns the map that maintains the Function classes that have information on declared methods.

  def self.get_functions_to_register
    self.name_and_add_class_object

    @@functions_to_register
  end

  # The Function class privates a convenient wrapper to store information about EvalFuncs, separating
  # out the methods that will be used on the frontend to get information on the method registered.

  class Function
    attr_accessor :method_name
    attr_reader :arity, :class_object

    def initialize class_object, method_name, arity
      @class_object = class_object
      @method_name = method_name
      @arity = arity
    end

    def required_args
      if @arity.is_a? Numeric
        @arity
      else
        @class_object.instance_method(@arity.to_sym).parameters.count {|x,y| x==:req}
      end
    end

    def optional_args
      if @arity.is_a? Numeric
        0
      else
        params = @class_object.instance_method(@arity.to_sym).parameters
        return -1 if params.any? {|x,y| x==:rest}
        params.count {|x,y| x==:opt}
      end
    end

    # This conveniently gives an instance of the class this Function wraps, so that on the Java end
    # it is trivial to get the object against which method calls can be made.

    def get_receiver
      @class_object.new
    end

    # This is useful for identifying the subclass Java is dealing with (EvalFunc, FilterFunc, etc)

    def name
      return self.class.to_s
    end
  end

  class EvalFunc < Function
    def initialize class_object, method_name, arity, schema_or_func
      super class_object, method_name, arity
      @schema_or_func = schema_or_func
    end

    # This is the function that will be used from Java to get the proper schema of the output.
    # Given that users have two options, output_schema or output_schema_function, this method
    # detects which and acts appropriately. It must be given an instance of the EvalFunc (generally
    # the result of "get_receiver") in the case of an output_schema_function so that it can evaluate
    # the output Schema based on the input Schema.

    def schema input_schema, class_instance
      if !@schema_or_func
         return Schema.bytearray
      elsif @schema_or_func.is_a? String
         return Schema.new @schema_or_func
      elsif @schema_or_func.is_a? Schema
         return @schema_or_func
      else
         func = @schema_or_func
         func = @class_object.instance_method(func) if func.is_a? Symbol
         return func.bind(class_instance).call input_schema
      end
    end
  end
end

# This is the base class used for Algebraic and Accumulator functions. The reason for the different
# implementation is because there is more structure in these cases. In the case of general EvalFuncs,
# a method is equivalent to a UDF. In the case of Algebraic and Accumulator UDFs, however, a class is
# equivalent to a UDF. Thus, instead of keeping track of methods added, we keep track of classes
# that extend our Algebraic and Accumulator UDF base classes.

class ComplexUdfBase
  # As with the basic PigUdf, there is a class method "output_schema" which defines the schema for the class.
  # This method can be called anywhere (as there is not the issue of multiple UDFs to worry about). If it is not
  # called, it will have return type bytearray.

  def self.output_schema schema
    @schema = schema
  end

  class << self
    alias :outputSchema :output_schema
  end

  # This returns the schema, or in the case that one was not supplied, a Schema of bytearray.

  def self.get_output_schema
    Schema.new(@schema||Schema.bytearray)
  end

  # Since a class = a UDF, in this case it makes sense to traverse the tree of decendant classes
  # in order to pull all of the registered classes. It's important to note

  def self.classes_to_register
    classes = {}
    ObjectSpace.each_object(Class) do |c|
      classes[c.to_s] = c if c.ancestors.include?(self) and (c != self)
    end
    classes
  end

  # This is a method that can be used by Pig to ensure that all of the necessary methods are present, so that
  # the function will throw an error on parsing instead of on execution. This is a shell implementation
  # to ensure that necessary_methods is called by a subclass, which will then generate the proper implementation.

  def self.check_if_necessary_methods_present
    throw "Need to declare the methods that should be present"
  end

  # This is a method that, if called at the class level, defines a set of methods that must be called
  # by any child classes (ie UDFs).

  def self.necessary_methods *m
    self.instance_eval "def self.check_if_necessary_methods_present; #{Array(m).inspect}.all? { |m| self.method_defined? m }; end"
  end
end

# This is the class that any Accumulator UDF must extend. The necessary_methods call ensures that all
# child classes have the necessary methods implemented. AccumulatorPigUdfs support dynamic output_schema.
# To do so, register a block with the schema function, as so:
# output_schema do |input|
#  return input
# end
#
# In the case of a non-dyanamic output schema, it's possible to stil just set output_schema "long".
#
# an example of an accumulator UDF is:
#
# class SUM < AccumulatorPigUdf
#   output_schema "long"
#
#   def exec input
#     @res ||= 0
#     input.flatten.inject(:+)
#   end
#   def get
#     @res
#   end
# end

class AccumulatorPigUdf < ComplexUdfBase
  def self.output_schema schema=nil, &blk
    if block_given?
      throw "Can specify block or schema but not both!" if schema
      throw "Block must accept one argument!" if blk.arity != 1
      @schema = blk
    else
      @schema = schema
    end
  end

  class << self
    alias :outputSchema :output_schema
  end

  def self.get_output_schema input_schema=nil
    if input_schema && @schema.class == Proc
      @schema.call input_schema
    else
      Schema.new(@schema||Schema.bytearray)
    end
  end

  necessary_methods :exec, :get
end

# This is the class that any Accumulator UDF must extend. The necessary_methods call ensures that all
# child classes have the necessary methods implemented.
#
# an example of an Algebraic UDF is:
#
# class Count < AlgebraicPigUdf
#   output_schema "long"
#
#   def initial t
#     t.nil? ? 0 : 1
#   end
#
#   def intermed t
#     return 0 if t.nil?
#     return t.flatten.inject(:+)
#   end
#
#   def final t
#     return intermed(t)
#   end
# end


class AlgebraicPigUdf < ComplexUdfBase
  necessary_methods :initial, :intermed, :final
end
