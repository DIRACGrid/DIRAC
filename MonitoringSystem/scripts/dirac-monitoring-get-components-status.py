#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/MonitoringSystem/scripts/dirac-monitoring-get-components-status.py,v 1.1 2009/02/26 14:25:02 acasajus Exp $
# File :   dirac-admin-submit-pilot-for-job
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac-monitoring-get-components-status.py,v 1.1 2009/02/26 14:25:02 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

fieldsToShow = ( 'ComponentName', 'Type', 'Host', 'Port', 'Status', 'Message' )

result = DIRAC.gMonitor.getComponentsStatusWebFormatted( sortingList = [ [ 'ComponentName', 'ASC' ] ] )
if not result[ 'OK' ]:
  print "ERROR: %s" % result[ 'Message' ]
  sys.exit(1)
paramNames = result[ 'Value' ][ 'ParameterNames' ]
records = result[ 'Value' ][ 'Records' ]
fieldLengths = []
for param in paramNames:
  fieldLengths.append( len( param ) )

for record in records:
  for i in range( len( record ) ):
    if paramNames[i] in fieldsToShow:
      fieldLengths[i] = max( fieldLengths[ i ], len( str( record[i] ) ) )
#Print time!
line = []
sepLine = []
for i in range( len( paramNames ) ):
  param = paramNames[i]
  if param in fieldsToShow:
    line.append( "%s%s" % ( param, " " * ( fieldLengths[i] - len( param ) ) ) )
    sepLine.append( "-" * fieldLengths[i])
print "|".join( line )
sepLine = "+".join( sepLine )
print sepLine
for record in records:
  line = []
  for i in range( len( record ) ):
    if paramNames[i] in fieldsToShow:
      val = str( record[i] )
      line.append( "%s%s" % ( val, " " * ( fieldLengths[i] - len( val ) ) ) )
  print "|".join( line )
  #print sepLine
