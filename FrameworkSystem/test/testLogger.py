__RCSID__ = "$Id$"

# FIXME: should be rewritten as a real unitttest

import DIRAC
from DIRAC import gLogger
gLogger.setLevel( 'DEBUG' )

DIRAC.gLogger.initialize('test_gLogger','/testSectionDebug')


try:
  for i in None:
    pass
except:
  pass

testList = [{ 'method'    : DIRAC.gLogger.always,
              'arguments' : ( ( "This is a always message" ), ),
              'output'    : True
            },
            { 'method'    : DIRAC.gLogger.info,
              'arguments' : ( ( "This is a info message" ), ),
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

