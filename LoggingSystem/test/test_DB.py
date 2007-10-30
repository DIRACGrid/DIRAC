"""  This program tests that the Logging DB can be actually queried from DIRAC 
"""
from dirac import DIRAC
from DIRAC.LoggingSystem.DB.SystemLoggingDB import SystemLoggingDB

DBpoint=SystemLoggingDB()

testList = [{ 'method': DBpoint.getMessagesByFixedText,
              'arguments': (( 'error message 1' ),),
              'outputType': 'Type',
              'output': True
            },
            { 'method': DBpoint.getMessagesByFixedText,
              'arguments': (( [ 'error message 1', 'error message 4' ] ),),
              'outputType': 'Type',
              'output': True
            },
            { 'method': DBpoint.getMessagesByDate,
              'arguments': ( '2007-08-24', '2007-08-26' ,),
              'outputType': 'Type',
              'output': True
            },
            { 'method': DBpoint.getMessagesBySite,
              'arguments': (( 'Site1' ),),
              'outputType': 'Type',
              'output': True
            },
            { 'method': DBpoint.getMessagesBySite,
              'arguments': (([ 'Site1', 'Site2' ]),),
              'outputType': 'Type',
              'output': True
            },
  ]

testdict = { 'SystemLoggingDB': testList,}

  
DIRAC.Tests.run( testdict, 'DIRAC.Information.Logger.DB' )

DIRAC.exit()
