""" Test class for plugins
"""

__RCSID__ = "$Id$"

# imports
import unittest

# sut
from DIRAC.Core.Utilities.Time import *


#
# def testTime():
#
#   curDateTime = DIRAC.Time.dateTime()
#   curDate     = DIRAC.Time.date( curDateTime )
#   curTime     = DIRAC.Time.time( curDateTime )
#   curDateTimeString = DIRAC.Time.toString( curDateTime )
#   curDateString     = DIRAC.Time.toString( curDate )
#   curTimeString     = DIRAC.Time.toString( curTime )
#
#   newDateTimeString = curDateString + " " + curTimeString
#
#   interval    = DIRAC.Time.timeInterval( curDateTime, curTime )
#   inDateTime  = curDateTime + curTime / 2
#   outDateTime = curDateTime + curTime * 2
#
#   testToString = [{ 'method'    : DIRAC.Time.toString,
#                     'arguments' : ( curDateTime, ),
#                     'output'    : newDateTimeString
#                   },]
#
#   testFromString1 = [{ 'method'    : DIRAC.Time.fromString,
#                        'arguments' : ( curDateTimeString, ),
#                        'output'    : curDateTime
#                      },]
#
#   testFromString2 = [{ 'method'    : DIRAC.Time.fromString,
#                        'arguments' : ( curDateString, ),
#                        'output'    : curDate
#                      },]
#
#   testFromString3 = [{ 'method'    : DIRAC.Time.fromString,
#                        'arguments' : ( curTimeString, ),
#                        'output'    : curTime
#                      },]
#
#   testInInterval  = [{ 'method'    : interval.includes,
#                        'arguments' : ( inDateTime, ),
#                        'output'    : True
#                      },]
#
#   testOutInterval  = [{ 'method'    : interval.includes,
#                         'arguments' : ( outDateTime, ),
#                         'output'    : False
#                      },]
#
#
#   testdict = { 'DIRAC.Time.timeToString'               : testToString,
#                'DIRAC.Time.timeFromString( DateTime )' : testFromString1,
#                'DIRAC.Time.timeFromString( Date )'     : testFromString2,
#                'DIRAC.Time.timeFromString( Time )'     : testFromString3,
#                'DIRAC.Time.timeInterval( In )'     : testInInterval,
#                'DIRAC.Time.timeInterval( Out )'    : testOutInterval }
#
# #   DIRAC.Tests.run( testdict )
#
#   return True
#
# def testNetwork():
#
#   testAllInterfaces = [{ 'method'    : DIRAC.Network.getAllInterfaces,
#                          'arguments' : ( ),
#                          'output'    : False
#                        }, ]
#
#   testAddressFromInterface = [{ 'method'    : DIRAC.Network.getAddressFromInterface,
#                                 'arguments' : ( 'lo', ),
#                                 'output'    : '127.0.0.1'
#                               }, ]
#
#
#   testdict = { 'DIRAC.Network.getAllInterfaces'        : testAllInterfaces,
#                'DIRAC.Network.getAddressFromInterface' : testAddressFromInterface }
#
#   testdict = { 'DIRAC.Network.getAddressFromInterface' : testAddressFromInterface }
#
# #   DIRAC.Tests.run( testdict )
#   return True
#
# def writeToStdout( iIndex, sLine ):
#   if iIndex == 0: # stdout
#     DIRAC.gLogger.info( 'stdout:', sLine )
#   if iIndex == 1: # stderr
#     DIRAC.gLogger.error( 'stderr:', sLine )
#
# def failingWriteToStdOut( iIndex ):
#   pass
#
#
#
# testTime = [{ 'method'    : testTime,
#               'arguments' : ( ),
#               'output'    : True
#             },]
#
# testNetwork = [{ 'method'    : testNetwork,
#                  'arguments' : ( ),
#                  'output'    : True
#                },]
#
# testReturnValues = [{ 'method'    : DIRAC.ReturnValues.S_OK,
#                       'arguments' : ( 24, ),
#                       'output'    : { 'OK': 1, 'Value': 24}
#                     },
#                     { 'method'    : DIRAC.ReturnValues.S_ERROR,
#                       'arguments' : ( 24, ),
#                       'output'    : { 'OK': 0, 'Message': '24'}
#                     },]
#
