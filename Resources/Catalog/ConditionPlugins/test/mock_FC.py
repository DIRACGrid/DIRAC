""" Module that contains a mock object of the FileCatalog(s)
"""

from mock import MagicMock

fileCatalogMock = MagicMock()

fileCatalogMock.listDirectory.return_value = {'OK': True,
                                              'Value': { 'Failed': {},
                                                         'Successful': {'/this/is/dir1/':{'Datasets': {},
                                                                                          'Files': {},
                                                                                          'Links': {},
                                                                                          'SubDirs': {}},
                                                                        '/this/is/dir2/':{'Datasets': {},
                                                                                          'Files': {},
                                                                                          'Links': {},
                                                                                          'SubDirs': {}}}
                                                       }
                                             }
