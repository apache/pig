import unittest
import controller
import StringIO
import sys

from datetime import datetime

class TestDeserializer( unittest.TestCase ):
    def test__no_params(self):
        input = ""
        expected_output = []
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__chararray(self):
        input = "C1234"
        expected_output = ["1234"]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__empty_chararray(self):
        input = "C"
        expected_output = [""]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__null_chararray(self):
        input = "|-_"
        expected_output = [None]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__boolean(self):
        input = "Btrue"
        expected_output = [True]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__datetime_dateutil(self):
        controller.USE_DATEUTIL = True
        try:
            from dateutil.tz import *
            from dateutil import parser
            input = "T2008-09-03T20:56:35.450+00:00"
            expected_output = [ datetime(2008, 9, 3, 20, 56, 35, 450000, tzinfo=tzutc()) ]
            out = controller.deserialize_input(input)
            self.assertEquals(expected_output, out)
        except ImportError:
            pass

    def test__datetime_datetime(self):
        controller.USE_DATEUTIL = False

        input = "T2008-09-03T20:56:36.444+00:00"
        expected_output = [ datetime(2008, 9, 3, 20, 56, 36, 444000) ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

        controller.USE_DATEUTIL = True


    def test__two_elements(self):
        input = "C032550737A79C543|\t_I970916083725"
        expected_output = [ "032550737A79C543", 970916083725 ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__three_elements_one_null(self):
        input = "C032550737A79C543|\t_I970916083725|\t_|-_"
        expected_output = [ "032550737A79C543", 970916083725, None ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__bag(self):
        input = "|{_|(_C79C543|,_I9709|,_Crichard keith|)_|,_|(_C79C543|,_I97091|,_Cmicrosoft works|)_|,_|(_C79C543|,_I970|,_Csearch engines|)_|}_"
        expected_output = [ [ ("79C543", 9709, "richard keith"),
                              ("79C543", 97091, "microsoft works"),
                              ("79C543", 970, "search engines") ] ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__two_elements_bags(self):
        input = "|{_|(_C79C543|,_D97091608|,_Crichard keith frazine|)_|,_|(_C79C543|,_D97091609|,_Cmicrosoft works|)_|}_|\t_|{_|(_C79C543|,_D97091608|)_|,_|(_C79C543|,_D9709160|)_|}_"
        expected_output =  [[("79C543",float(97091608),"richard keith frazine"),
                             ("79C543", float(97091609), "microsoft works") ],
                            [("79C543", float(97091608)),
                             ("79C543", float(9709160))]]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__atomic_mixed_with_complex(self):
        input = "|{_|(_C79C543|,_L970916083725|,_Crichard keith frazine|)_|,_|(_C79C543|,_L970916095254|,_Cmicrosoft works|)_|}_|\t_I323|\t_|{_|(_C79C543|,_L970916083725|,_Crichard keith frazine|)_|,_|(_C79C543|,_L970916095254|,_Cmicrosoft works|)_|}_"
        expected_output = [ [ ("79C543", 970916083725, "richard keith frazine"),
                              ("79C543", 970916095254, "microsoft works") ],
                            323,
                            [ ("79C543", 970916083725, "richard keith frazine"),
                              ("79C543", 970916095254, "microsoft works") ] ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__short_tuple(self):
        input = "|(_I1|)_"
        expected_output = [ (1,) ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__nested_tuple(self):
        input = "|(_|(_I123|,_Cabc|)_|,_|(_Cdef|,_I456|)_|)_"
        expected_output = [ ( (123, "abc"), ("def", 456) ) ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__map(self):
        input = "|[_Cname#CJohn|,_Cext#I5555|]_"
        expected_output = [ {"name":u"John",
                             "ext":5555} ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__short_field_map(self):
        input = "|[_Cn#CJohn|,_Ce#C5555|]_"
        expected_output = [ {"n":u"John",
                             "e":u"5555"} ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__complex_map(self):
        input = "|[_CD#|[_CA#I1|,_CB#CE|]_|,_CC#CF|]_"
        expected_output = [ { u"D": { u"A": 1,
                                     u"B": u"E"
                                   },
                              u"C": u"F"
                            } ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__all_types(self):
        input = "A123|\t_Btrue|\t_Cabc|\t_D4.0|\t_F5.0|\t_I32|\t_L45"
        expected_output = [ b"123", True, u"abc", 4.0, 5.0, 32, 45L ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__bad_types(self):
        input = "K123"
        self.assertRaises(Exception, controller.deserialize_input, [input])

    def test__error(self):
        self.maxDiff = None
        input = "|[_Cspec#|{_|(_|[_Croles#|{_|(_Chadoop-namenode|)_|,_|(_Chadoop-jobtracker|)_|,_|(_Cpig-master|)_|}_|,_Cnum_instances#I1|]_|)_|,_|(_|[_Croles#|{_|(_Chadoop-datanode|)_|,_|(_Chadoop-tasktracker|)_|}_|,_Cnum_instances#I1|]_|)_|}_|,_Chardware#Cm1.large|,_Caccount_id#I1234567|,_Clocation#Cus-east-1b|,_Cstatus#Cdestroyed|,_Cimage#Cus-east-1/ami-1234|,_Cjclouds_name#Cmhcdevelopment_1234|,_Cinstances#|{_|(_|[_Cprivate_address#C10.10.10.10|,_Croles#|{_|(_Chadoop-datanode|)_|,_|(_Chadoop-tasktracker|)_|}_|,_Cpublic_address#Cec2-10-10-10-10.compute-1.amazonaws.com|,_Cinstance_id#Cus-east-1/i-1234|]_|)_|,_|(_|[_Cprivate_address#C10.10.10.10|,_Croles#|{_|(_Chadoop-namenode|)_|,_|(_Chadoop-jobtracker|)_|,_|(_Cpig-master|)_|}_|,_Cpublic_address#Cec2-10-10-10-10.compute-1.amazonaws.com|,_Cinstance_id#Cus-east-1/i-4321|]_|)_|}_|,_Cstop_timestamp#|-_|,_Cplan_code#Cstandard|,_C_id#I1234567890|,_Crunning_timestamp#|-_|,_Cuser_id#I1234|,_Cstart_timestamp#CTue Oct 25 19:26:18 UTC 2011|]_"
        expected_output = [ {u'status': u'destroyed',
                             u'start_timestamp': u'Tue Oct 25 19:26:18 UTC 2011',
                             u'user_id': 1234,
                             u'account_id': 1234567,
                             u'running_timestamp': None,
                             u'image': u'us-east-1/ami-1234',
                             u'hardware': u'm1.large',
                             u'instances': [ ( { u'private_address':u'10.10.10.10',
                                                 u'roles' : [ ( u'hadoop-datanode', ),
                                                              ( u'hadoop-tasktracker',) ],
                                                 u'public_address':u'ec2-10-10-10-10.compute-1.amazonaws.com',
                                                 u'instance_id':u'us-east-1/i-1234'
                                               },
                                             ),
                                             ( {
                                                 u'private_address':u'10.10.10.10',
                                                 u'roles' : [ ( u'hadoop-namenode', ),
                                                              ( u'hadoop-jobtracker', ),
                                                              ( u'pig-master', ) ],
                                                 u'public_address' : u'ec2-10-10-10-10.compute-1.amazonaws.com',
                                                 u'instance_id' : u'us-east-1/i-4321'
                                               },
                                             )
                                           ],
                             u'plan_code': u'standard',
                             u'location': u'us-east-1b',
                             u'_id': 1234567890,
                             u'spec': [ ( { u'roles' : [ (u'hadoop-namenode', ),
                                                         (u'hadoop-jobtracker', ),
                                                         (u'pig-master', ) ],
                                            u'num_instances' : 1,
                                          },
                                        ),
                                        ( { u'roles' : [ (u'hadoop-datanode',),
                                                         (u'hadoop-tasktracker',) ],
                                            u'num_instances' : 1,
                                          },
                                        )
                                      ],
                             u'jclouds_name': u'mhcdevelopment_1234',
                             u'stop_timestamp': None}]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__empty_string(self):
        input = "C"
        expected_output = [ "" ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

    def test__string_with_hash(self):
        input = "Cabc#!,|g|,(){}"
        expected_output = [ "abc#!,|g|,(){}" ]
        out = controller.deserialize_input(input)
        self.assertEquals(expected_output, out)

class TestSerializeOutput( unittest.TestCase ):
    def test__chararray(self):
        input = "1234"
        expected_output = "1234"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__empty_chararray(self):
        input = ""
        expected_output = ""
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__null_chararray(self):
        input = None
        expected_output = "|-_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__num(self):
        input = 1234
        expected_output = "1234"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__bool_true(self):
        input = True
        expected_output = "true"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__bool_false(self):
        input = False
        expected_output = "false"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__datetime(self):
        d = datetime.now()
        input = (d,)
        expected_output = "|(_%s|)_" % d.isoformat()
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__tuple(self):
        input = (1234, "abc")
        expected_output = "|(_1234|,_abc|)_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__short_tuple(self):
        input = (1,)
        expected_output = "|(_1|)_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__bag(self):
        input = [1234, "abc"]
        expected_output = "|{_1234|,_abc|}_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__nested_tuple(self):
        input = [(32,12,'abc'), 32, ['abc', 'def', 'ghi']]
        expected_output = "|{_|(_32|,_12|,_abc|)_|,_32|,_|{_abc|,_def|,_ghi|}_|}_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__map(self):
        input = {'a': 1, 'b':'z'}
        expected_output = "|[_a#1|,_b#z|]_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

    def test__bug(self):
        input = (None, 32, 98765432109876543210L)
        expected_output = "|(_|-_|,_32|,_98765432109876543210|)_"
        out = controller.serialize_output(input)
        self.assertEquals(expected_output, out)

class TestReadInput( unittest.TestCase ):
    def test__multiline_record(self):
        cont = controller.PythonStreamingController()
        inputio = StringIO.StringIO()
        inputio.write('12\n')
        inputio.write('34\n')
        inputio.write('5|_\n')
        inputio.seek(0)

        cont.input_stream = inputio
        cont.output_stream = sys.stdout
        out = cont.get_next_input()

        self.assertEquals('12\n34\n5', out)

    def test__complexmultiline_record(self):
        cont = controller.PythonStreamingController()
        inputio = StringIO.StringIO()
        inputio.write('|{_|(_32|,_12|,_a\n')
        inputio.write('bc|)_|,_32|,_|{_ab\n')
        inputio.write('c|,_def|,_gh\n')
        inputio.write('i|}_|}_|_\n')
        inputio.seek(0)

        cont.input_stream = inputio
        cont.output_stream = sys.stdout
        out = cont.get_next_input()

        self.assertEquals('|{_|(_32|,_12|,_a\nbc|)_|,_32|,_|{_ab\nc|,_def|,_gh\ni|}_|}_', out)
