""" A mock of the DataManager, used for testing purposes
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

from mock import MagicMock

from DIRAC import S_OK

dm_mock = MagicMock()
dm_mock.getReplicas.return_value = S_OK({'Successful': {'/a/lfn/1.txt': {'SE1': '/a/lfn/at/SE1.1.txt',
                                                                         'SE2': '/a/lfn/at/SE2.1.txt'},
                                                        '/a/lfn/2.txt': {'SE1': '/a/lfn/at/SE1.1.txt'}},
                                         'Failed': {}})
dm_mock.getActiveReplicas.return_value = dm_mock.getReplicas.return_value
dm_mock.getReplicasForJobs.return_value = dm_mock.getReplicas.return_value
dm_mock.getCatalogFileMetadata.return_value = {'OK': True, 'Value': {'Successful': {'pippo': 'metadataPippo'},
                                                                     'Failed': None}}
dm_mock.removeFile.return_value = {'OK': True, 'Value': {'Failed': False}}
dm_mock.putStorageDirectory.return_value = {'OK': True, 'Value': {'Failed': False}}
dm_mock.addCatalogFile.return_value = {'OK': True, 'Value': {'Failed': False}}
dm_mock.putAndRegister.return_value = {'OK': True, 'Value': {'Failed': False}}
dm_mock.getFile.return_value = {'OK': True, 'Value': {'Failed': False}}
