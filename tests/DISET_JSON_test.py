from DIRAC.Core.Utilities import DEncode
import json
import datetime
import unittest

#Thes are variables we are going to use
test_tuple = (1,2,3)
dict_tuple = {'__tuple__': True, 'items': test_tuple}
test_long = long(6)
dict_long = {'__long__': True, 'value': test_long}
test_dateTime = datetime.datetime(1970, 1, 1, 12, 57, 36, 99997)
dateTimeTuple = ( test_dateTime.year, test_dateTime.month, test_dateTime.day, test_dateTime.hour,
                  test_dateTime.minute, test_dateTime.second,
                  test_dateTime.microsecond, test_dateTime.tzinfo )
dict_dateTime = {'__dateTime__':True, 'items':dateTimeTuple}
test_date = datetime.date(1789, 7, 14)
dateTuple = dateTuple = ( test_date.year, test_date.month, test_date. day )
dict_date = {'__date__':True, 'items':dateTuple}
test_time = datetime.time(8, 30, 43)
timeTuple = ( test_time.hour, test_time.minute, test_time.second, test_time.microsecond,
              test_time.tzinfo )
dict_time = {'__time__':True, 'items':timeTuple}
test_list = [test_tuple, test_long, test_dateTime, test_date, test_time]
test_dict = {'t':test_tuple, 'l':test_long, 'DT':test_dateTime, 'D':test_date, 'T':test_time}

class testDEncodeJSON(unittest.TestCase):
    """Test case used to test DEncode.py functions"""

    def test_hintParticularTypes( self ):
        """Testing the 'DEncode.hintParticularTypes()' function. This function
        detects tuples and longs and replaces them with dictionaries. This
        allows us to prserve these data types. By default, 'json.dumps()' encodes
        tuples into arrays, (like python lists) and longs into int numbers
        (like python ints). By using directly 'json.loads()', without
        'DEncode.hintParticularTypes()', arrays are decoded into lists (so we
        lose our tuples) and int numbers into ints (then we also lose long ints)."""

        self.assertDictEqual(DEncode.hintParticularTypes(test_tuple),
                             dict_tuple)
        self.assertDictEqual(DEncode.hintParticularTypes(test_long),
                             dict_long)
        self.assertEqual(DEncode.hintParticularTypes(test_dateTime), dict_dateTime)
        self.assertEqual(DEncode.hintParticularTypes(test_date), dict_date)
        self.assertEqual(DEncode.hintParticularTypes(test_time), dict_time)
        self.assertEqual(DEncode.hintParticularTypes(test_list),
                         [dict_tuple,dict_long, dict_dateTime, dict_date, dict_time])
        self.assertEqual(DEncode.hintParticularTypes(test_dict),
                         {'t':dict_tuple, 'l':dict_long, 'DT':dict_dateTime,
                          'T':dict_time, 'D':dict_date})
        self.assertEqual(DEncode.hintParticularTypes([1,2,3]), [1,2,3])

    def test_DetectHintedParticularTypes( self ):
        self.assertEqual(DEncode.DetectHintedParticularTypes(dict_tuple), test_tuple)
        self.assertEqual(DEncode.DetectHintedParticularTypes(dict_long), test_long)
        self.assertEqual(DEncode.DetectHintedParticularTypes([dict_tuple,dict_long,
                         dict_dateTime,dict_date,dict_time]), test_list)
        self.assertEqual(DEncode.DetectHintedParticularTypes({'t':dict_tuple,
                         'l':dict_long,'DT':dict_dateTime,'D':dict_date,'T':dict_time}),
                         test_dict)

    def test_encode( self ):
        self.assertEqual(DEncode.encode(test_tuple), json.dumps(dict_tuple))
        self.assertEqual(DEncode.encode(test_long), json.dumps(dict_long))
        self.assertEqual(DEncode.encode(test_list), json.dumps([dict_tuple,dict_long,
                                                                dict_dateTime,
                                                                dict_date,
                                                                dict_time]))

        ###############################################################################
        #These tests will fail, because the encoded strings will be created from      #
        #dictionaries, in which the order of the values is not taken into account.    #
        #self.assertEqual(DEncode.encode(test_dict), json.dumps({'t':dict_tuple,      #
        #                                                        'l':dict_long,       #
        #                                                        'DT':dict_dateTime,  #
        #                                                        'D':dict_date,       #
        #                                                        'T':dict_time}))     #
        #self.assertEqual(DEncode.encode({'x':3.1415265, 'y':6.02*10**23}),           #
        #                               json.dumps({'x':3.1415265, 'y':6.02*10**23})) #
        ###############################################################################
        
        self.assertEqual(DEncode.encode([1,2.5,True,False]), json.dumps([1,2.5,True,False]))
        self.assertEqual(DEncode.encode(None), json.dumps(None))
        self.assertEqual(DEncode.encode("This is a string"), json.dumps("This is a string"))

    def test_decode(self):
        self.assertEqual(DEncode.decode(DEncode.encode(test_tuple)), test_tuple)
        self.assertEqual(DEncode.decode(DEncode.encode(test_long)), test_long)
        self.assertEqual(DEncode.decode(DEncode.encode(test_list)), test_list)
        self.assertEqual(DEncode.decode(DEncode.encode(test_dict)), test_dict)
        self.assertEqual(DEncode.decode(DEncode.encode("Another string")), "Another string")
        self.assertEqual(DEncode.decode(DEncode.encode(None)), None)
        self.assertEqual(DEncode.decode(DEncode.encode(True)), True)
        self.assertEqual(DEncode.decode(DEncode.encode(False)), False)
        self.assertEqual(DEncode.decode(DEncode.encode(1)), 1)
        self.assertEqual(DEncode.decode(DEncode.encode(6.02*10**23)), 6.02*10**23)

if __name__ == "__main__":
    unittest.main()

    #suite = unittest.TestLoader().loadTestsFromTestCase(testDEncodeJSON)
    #unittest.TextTestRunner(verbosity=2).run(suite)
