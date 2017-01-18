""" A mock of the DataManager, used for testing purposes
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

from mock import MagicMock

from DIRAC import S_OK

dm_mock = MagicMock()
dm_mock.getReplicas.return_value = S_OK( {'Successful': {'/a/lfn/1.txt':{'SE1':'/a/lfn/at/SE1.1.txt',
                                                                         'SE2':'/a/lfn/at/SE2.1.txt'},
                                                         '/a/lfn/2.txt':{'SE1':'/a/lfn/at/SE1.1.txt'}},
                                          'Failed':{}} )
