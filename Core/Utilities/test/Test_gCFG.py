# FIXME: should be rewritten as real unittest

# __RCSID__ = "$Id$"
#
# import DIRAC
# from DIRAC.Core.Utilities.CFG import CFG
#
# DIRAC.gLogger.initialize('test_gConfig','/testSectionDebug')
#
# testconfig = '%s/DIRAC/ConfigurationSystem/test/test.cfg' % DIRAC.rootPath
# dumpconfig = '%s/DIRAC/ConfigurationSystem/test/dump.cfg' % DIRAC.rootPath
#
# cfg1 = CFG()
# cfg1.loadFromFile( testconfig )
#
# fd = file( testconfig )
# cfg1String = fd.read()
# fd.close()
#
# cfg2 = CFG()
# cfg2.loadFromBuffer( cfg1.serialize() )
#
# cfg3 = cfg1.mergeWith( cfg2 )
#
# testList = [{ 'method'    : DIRAC.gConfig.loadFile,
#               'arguments' : ( testconfig, ),
#               'output'    : {'OK': True, 'Value': ''}
#             },
#             { 'method'    : DIRAC.gConfig.dumpLocalCFGToFile,
#               'arguments' : ( dumpconfig, ),
#               'output'    : {'OK': True, 'Value': ''}
#             },
#             { 'method'    : cfg1.serialize,
#               'arguments' : ( ),
#               'output'    : cfg1String
#             },
#             { 'method'    : cfg3.serialize,
#               'arguments' : ( ),
#               'output'    : cfg1String
#             }]
#
# testdict = { 'DIRAC.gConfig'               : testList,}
#
# DIRAC.Tests.run( testdict, 'DIRAC.gConfig.files' )
#
#
#
# # testList = [{ 'method'    : DIRAC.gConfig.get,
# #               'arguments' : ( '/testSection/test', ),
# #               'output'    : {'OK': True, 'Value': 'test'}
# #             },
# # #             { 'method'    : DIRAC.gConfig.get,
# # #              'arguments' : ( '/testSection/nonexisting','OK', ),
# # #              'output'    : {'OK': True, 'Value': 'test'}
# # #            },
# #              ]
# #
# # testdict = { 'DIRAC.gConfig'               : testList,}
# #
# #
# # DIRAC.Tests.run( testdict, 'DIRAC.gConfig' )

