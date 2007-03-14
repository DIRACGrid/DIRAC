# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/test/test_gCFG.py,v 1.2 2007/03/14 06:31:29 rgracian Exp $
__RCSID__ = "$Id: test_gCFG.py,v 1.2 2007/03/14 06:31:29 rgracian Exp $"

from dirac import DIRAC

DIRAC.gLogger.initialize('test_gConfig','/testSection')

DIRAC.gConfig.loadFile('./test.cfg')

testList = [{ 'method'    : DIRAC.gConfig.getOption,
              'arguments' : ( '/testSection/test', ),
              'output'    : {'OK': True, 'Value': 'test'}
            },             
             ]

testdict = { 'DIRAC.gConfig'               : testList,}

  
DIRAC.Tests.run( testdict, 'DIRAC.gConfig' )


DIRAC.exit()
