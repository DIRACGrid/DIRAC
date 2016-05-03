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


fileCatalogMock.getReplicas.return_value = {'OK': True,
                                            'Value':{'Failed':{},
                                                     'Successful':{'/this/is/file1.txt':{'FileType': 'TXT'},
                                                                   '/this/is/file2.foo.bar': {'FileType': 'FOO.BAR'}},
                                                    }
                                           }
