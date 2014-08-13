#! /usr/bin/env python
import sys
import os
import logging

from datetime import datetime
try:
    from dateutil import parser
    USE_DATEUTIL = True
except ImportError:
    USE_DATEUTIL = False

from pig_util import write_user_exception, udf_logging

FIELD_DELIMITER = ','
TUPLE_START = '('
TUPLE_END = ')'
BAG_START = '{'
BAG_END = '}'
MAP_START = '['
MAP_END = ']'
MAP_KEY = '#'
PARAMETER_DELIMITER = '\t'
PRE_WRAP_DELIM = '|'
POST_WRAP_DELIM = '_'
NULL_BYTE = "-"
END_RECORD_DELIM = '|_\n'
END_RECORD_DELIM_LENGTH = len(END_RECORD_DELIM)

WRAPPED_FIELD_DELIMITER = PRE_WRAP_DELIM + FIELD_DELIMITER + POST_WRAP_DELIM
WRAPPED_TUPLE_START = PRE_WRAP_DELIM + TUPLE_START + POST_WRAP_DELIM
WRAPPED_TUPLE_END = PRE_WRAP_DELIM + TUPLE_END + POST_WRAP_DELIM
WRAPPED_BAG_START = PRE_WRAP_DELIM + BAG_START + POST_WRAP_DELIM
WRAPPED_BAG_END = PRE_WRAP_DELIM + BAG_END + POST_WRAP_DELIM
WRAPPED_MAP_START = PRE_WRAP_DELIM + MAP_START + POST_WRAP_DELIM
WRAPPED_MAP_END = PRE_WRAP_DELIM + MAP_END + POST_WRAP_DELIM
WRAPPED_PARAMETER_DELIMITER = PRE_WRAP_DELIM + PARAMETER_DELIMITER + POST_WRAP_DELIM
WRAPPED_NULL_BYTE = PRE_WRAP_DELIM + NULL_BYTE + POST_WRAP_DELIM

TYPE_TUPLE = TUPLE_START
TYPE_BAG = BAG_START
TYPE_MAP = MAP_START

TYPE_BOOLEAN = "B"
TYPE_INTEGER = "I"
TYPE_LONG = "L"
TYPE_FLOAT = "F"
TYPE_DOUBLE = "D"
TYPE_BYTEARRAY = "A"
TYPE_CHARARRAY = "C"
TYPE_DATETIME = "T"
TYPE_BIGINTEGER = "N"
TYPE_BIGDECIMAL = "E"

END_OF_STREAM = TYPE_CHARARRAY + "\x04" + END_RECORD_DELIM
TURN_ON_OUTPUT_CAPTURING = TYPE_CHARARRAY + "TURN_ON_OUTPUT_CAPTURING" + END_RECORD_DELIM
NUM_LINES_OFFSET_TRACE = int(os.environ.get('PYTHON_TRACE_OFFSET', 0))

class PythonStreamingController:
    def __init__(self, profiling_mode=False):
        self.profiling_mode = profiling_mode

        self.input_count = 0
        self.next_input_count_to_log = 1

    def main(self,
             module_name, file_path, func_name, cache_path,
             output_stream_path, error_stream_path, log_file_name, is_illustrate_str):
        sys.stdin = os.fdopen(sys.stdin.fileno(), 'rb', 0)

        #Need to ensure that user functions can't write to the streams we use to
        #communicate with pig.
        self.stream_output = os.fdopen(sys.stdout.fileno(), 'wb', 0)
        self.stream_error = os.fdopen(sys.stderr.fileno(), 'wb', 0)

        self.input_stream = sys.stdin
        self.output_stream = open(output_stream_path, 'a')
        sys.stderr = open(error_stream_path, 'w')
        is_illustrate = is_illustrate_str == "true"

        sys.path.append(file_path)
        sys.path.append(cache_path)
        sys.path.append('.')

        logging.basicConfig(filename=log_file_name, format="%(asctime)s %(levelname)s %(message)s", level=udf_logging.udf_log_level)
        logging.info("To reduce the amount of information being logged only a small subset of rows are logged at the INFO level.  Call udf_logging.set_log_level_debug in pig_util to see all rows being processed.")

        input_str = self.get_next_input()

        try:
            func = __import__(module_name, globals(), locals(), [func_name], -1).__dict__[func_name]
        except:
            #These errors should always be caused by user code.
            write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
            self.close_controller(-1)

        if is_illustrate or udf_logging.udf_log_level != logging.DEBUG:
            #Only log output for illustrate after we get the flag to capture output.
            sys.stdout = open("/dev/null", 'w')
        else:
            sys.stdout = self.output_stream

        while input_str != END_OF_STREAM:
            should_log = False
            if self.input_count == self.next_input_count_to_log:
                should_log = True
                log_message = logging.info
                self.update_next_input_count_to_log()
            elif udf_logging.udf_log_level == logging.DEBUG:
                should_log = True
                log_message = logging.debug

            try:
                try:
                    if should_log:
                        log_message("Row %s: Serialized Input: %s" % (self.input_count, input_str))
                    inputs = deserialize_input(input_str)
                    if should_log:
                        log_message("Row %s: Deserialized Input: %s" % (self.input_count, unicode(inputs)))
                except:
                    #Capture errors where the user passes in bad data.
                    write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
                    self.close_controller(-3)

                try:
                    func_output = func(*inputs)
                    if should_log:
                        log_message("Row %s: UDF Output: %s" % (self.input_count, unicode(func_output)))
                except:
                    #These errors should always be caused by user code.
                    write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
                    self.close_controller(-2)

                output = serialize_output(func_output)
                if should_log:
                    log_message("Row %s: Serialized Output: %s" % (self.input_count, output))

                self.stream_output.write( "%s%s" % (output, END_RECORD_DELIM) )
            except Exception as e:
                #This should only catch internal exceptions with the controller
                #and pig- not with user code.
                import traceback
                traceback.print_exc(file=self.stream_error)
                sys.exit(-3)

            sys.stdout.flush()
            sys.stderr.flush()
            self.stream_output.flush()
            self.stream_error.flush()

            input_str = self.get_next_input()

    def get_next_input(self):
        input_stream = self.input_stream
        output_stream = self.output_stream

        input_str = input_stream.readline()

        while input_str.endswith(END_RECORD_DELIM) == False:
            input_str += input_stream.readline()

        if input_str == TURN_ON_OUTPUT_CAPTURING:
            logging.debug("Turned on Output Capturing")
            sys.stdout = output_stream
            return self.get_next_input()

        if input_str == END_OF_STREAM:
            return input_str

        self.input_count += 1

        return input_str[:-END_RECORD_DELIM_LENGTH]

    def update_next_input_count_to_log(self):
        """
        Want to log enough rows that you can see progress being made and see timings without wasting time logging thousands of rows.
        Show first 10 rows, and then the first 5 rows of every order of magnitude (10-15, 100-105, 1000-1005, ...)
        """
        if self.next_input_count_to_log < 10:
            self.next_input_count_to_log = self.next_input_count_to_log + 1
        elif self.next_input_count_to_log % 10 == 5:
            self.next_input_count_to_log = (self.next_input_count_to_log - 5) * 10
        else:
            self.next_input_count_to_log = self.next_input_count_to_log + 1

    def close_controller(self, exit_code):
        sys.stderr.close()
        self.stream_error.write("\n")
        self.stream_error.close()
        sys.stdout.close()
        self.stream_output.write("\n")
        self.stream_output.close()
        sys.exit(exit_code)

def deserialize_input(input_str):
    if len(input_str) == 0:
        return []

    return [_deserialize_input(param, 0, len(param)-1) for param in input_str.split(WRAPPED_PARAMETER_DELIMITER)]

def _deserialize_input(input_str, si, ei):
    if ei - si < 1:
        #Handle all of the cases where you can have valid empty input.
        if ei == si:
            if input_str[si] == TYPE_CHARARRAY:
                return u""
            elif input_str[si] == TYPE_BYTEARRAY:
                return bytearray("")
            else:
                raise Exception("Got input type flag %s, but no data to go with it.\nInput string: %s\nSlice: %s" % (input_str[si], input_str, input_str[si:ei+1]))
        else:
            raise Exception("Start index %d greater than end index %d.\nInput string: %s\n, Slice: %s" % (si, ei, input_str[si:ei+1]))

    first = input_str[si]
    schema = input_str[si+1] if first == PRE_WRAP_DELIM else first

    if schema == NULL_BYTE:
        return None
    elif schema == TYPE_TUPLE or schema == TYPE_MAP or schema == TYPE_BAG:
        return _deserialize_collection(input_str, schema, si+3, ei-3)
    elif schema == TYPE_CHARARRAY:
        return unicode(input_str[si+1:ei+1], 'utf-8')
    elif schema == TYPE_BYTEARRAY:
        return bytearray(input_str[si+1:ei+1])
    elif schema == TYPE_INTEGER:
        return int(input_str[si+1:ei+1])
    elif schema == TYPE_LONG or schema == TYPE_BIGINTEGER:
        return long(input_str[si+1:ei+1])
    elif schema == TYPE_FLOAT or schema == TYPE_DOUBLE or schema == TYPE_BIGDECIMAL:
        return float(input_str[si+1:ei+1])
    elif schema == TYPE_BOOLEAN:
        return input_str[si+1:ei+1] == "true"
    elif schema == TYPE_DATETIME:
        #Format is "yyyy-MM-ddTHH:mm:ss.SSS+00:00" or "2013-08-23T18:14:03.123+ZZ"
        if USE_DATEUTIL:
            return parser.parse(input_str[si+1:ei+1])
        else:
            #Try to use datetime even though it doesn't handle time zones properly,
            #We only use the first 3 microsecond digits and drop time zone (first 23 characters)
            return datetime.strptime(input_str[si+1:si+24], "%Y-%m-%dT%H:%M:%S.%f")
    else:
        raise Exception("Can't determine type of input: %s" % input_str[si:ei+1])

def _deserialize_collection(input_str, return_type, si, ei):
    list_result = []
    append_to_list_result = list_result.append
    dict_result = {}

    index = si
    field_start = si
    depth = 0

    key = None

    # recurse to deserialize elements if the collection is not empty
    if ei-si+1 > 0:
        while True:
            if index >= ei - 2:
                if return_type == TYPE_MAP:
                    dict_result[key] = _deserialize_input(input_str, value_start, ei)
                else:
                    append_to_list_result(_deserialize_input(input_str, field_start, ei))
                break

            if return_type == TYPE_MAP and not key:
                key_index = input_str.find(MAP_KEY, index)
                key = unicode(input_str[index+1:key_index], 'utf-8')
                index = key_index + 1
                value_start = key_index + 1
                continue

            if not (input_str[index] == PRE_WRAP_DELIM and input_str[index+2] == POST_WRAP_DELIM):
                prewrap_index = input_str.find(PRE_WRAP_DELIM, index+1)
                index = (prewrap_index if prewrap_index != -1 else end_index)
                continue

            mid = input_str[index+1]

            if mid == BAG_START or mid == TUPLE_START or mid == MAP_START:
                depth += 1
            elif mid == BAG_END or mid == TUPLE_END or mid == MAP_END:
                depth -= 1
            elif depth == 0 and mid == FIELD_DELIMITER:
                if return_type == TYPE_MAP:
                    dict_result[key] = _deserialize_input(input_str, value_start, index - 1)
                    key = None
                else:
                    append_to_list_result(_deserialize_input(input_str, field_start, index - 1))
                field_start = index + 3

            index += 3

    if return_type == TYPE_MAP:
        return dict_result
    elif return_type == TYPE_TUPLE:
        return tuple(list_result)
    else:
        return list_result

def serialize_output(output, utfEncodeAllFields=False):
    """
    @param utfEncodeStrings - Generally we want to utf encode only strings.  But for
        Maps we utf encode everything because on the Java side we don't know the schema
        for maps so we wouldn't be able to tell which fields were encoded or not.
    """

    output_type = type(output)

    if output is None:
        return WRAPPED_NULL_BYTE
    elif output_type == tuple:
        return (WRAPPED_TUPLE_START +
                WRAPPED_FIELD_DELIMITER.join([serialize_output(o, utfEncodeAllFields) for o in output]) +
                WRAPPED_TUPLE_END)
    elif output_type == list:
        return (WRAPPED_BAG_START +
                WRAPPED_FIELD_DELIMITER.join([serialize_output(o, utfEncodeAllFields) for o in output]) +
                WRAPPED_BAG_END)
    elif output_type == dict:
        return (WRAPPED_MAP_START +
                WRAPPED_FIELD_DELIMITER.join(['%s%s%s' % (k.encode('utf-8'), MAP_KEY, serialize_output(v, True)) for k, v in output.iteritems()]) +
                WRAPPED_MAP_END)
    elif output_type == bool:
        return ("true" if output else "false")
    elif output_type == bytearray:
        return str(output)
    elif output_type == datetime:
        return output.isoformat()
    elif utfEncodeAllFields or output_type == str or output_type == unicode:
        #unicode is necessary in cases where we're encoding non-strings.
        return unicode(output).encode('utf-8')
    else:
        return str(output)

if __name__ == '__main__':
    controller = PythonStreamingController()
    controller.main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
