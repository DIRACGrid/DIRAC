# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/test/test_Logger.py,v 1.3 2007/10/01 17:09:08 mseco Exp $
__RCSID__ = "$Id: test_Logger.py,v 1.3 2007/10/01 17:09:08 mseco Exp $"
"""
"""
from dirac import DIRAC

DIRAC.gLogger.initialize('test_gLogger','/testSectionDebug')


try:
  for i in None:
    pass
except:
  pass

# for i in range(100):
#   DIRAC.gLogger.info( "test %s" % i )

testList = [{ 'method'    : DIRAC.gLogger.always,
              'arguments' : ( ( "This is a always message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.info,
              'arguments' : ( ( "This is a vital message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.verbose,
              'arguments' : ( ( "This is a info message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.debug,
              'arguments' : ( ( "This is a debug message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.warn,
              'arguments' : ( ( "This is a warn message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.error,
              'arguments' : ( ( "This is a error message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.exception,
              'arguments' : ( ( "This is a exception message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.fatal,
              'arguments' : ( ( "This is a fatal message" ), ),
              'output'    : True
            },
            ]
testdict = { 'DIRAC.gLogger'               : testList,}

  
DIRAC.Tests.run( testdict, 'DIRAC.Information.Logger' )

import time
time.sleep( 10 )

DIRAC.exit()
