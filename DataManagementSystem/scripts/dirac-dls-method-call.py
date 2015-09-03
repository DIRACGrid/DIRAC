'''
Created on Jun 9, 2015

@author: Corentin Berger
'''
from DIRAC.Core.Base import Script

lfn = None
name = None
fullFlag = False
after = None
before = None
status = None
errorCode = None

Script.registerSwitch( '', 'Full', 'Print full method call' )
Script.registerSwitch( 'm:', 'MethodName=', 'Name of method [%s]' % name )
Script.registerSwitch( 'a:', 'After=', 'Date, format be like 1999-12-31 [%s]' % after )
Script.registerSwitch( 'b:', 'Before=', 'Date, format be like 1999-12-31 [%s]' % before )
Script.registerSwitch( 'w:', 'Status=', 'Failed, Successful or Unknown [%s]' % status )
Script.registerSwitch( 'e:', 'ErrorCode=', 'An error code' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                    'WARNING : the maximum number of method call to get from database is 1000',
                                     'USAGE:',
                                     ' %s [OPTION|CFGFILE] LFN ' % Script.scriptName,
                                     'ARGUMENTS:',
                                     'At least one shall be given\nLFN: AN LFN NAME \nNAME : A method name' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "m" or switch[0].lower() == "methodname":
    name = switch[1]
  elif switch[0] == "a" or switch[0].lower() == "after":
    after = switch[1]
  elif switch[0] == "b" or switch[0].lower() == "before":
    before = switch[1]
  elif switch[0] == "w" or switch[0].lower() == "status":
    status = switch[1]
  elif switch[0].lower() == "full":
    fullFlag = True
  elif switch[0] == "e" or switch[0].lower() == "errorcode":
    errorCode = switch[1]

args = Script.getPositionalArgs()
if args :
  lfn = args[0]

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

def printMethodCallLFN( call, lfn, full = False, status = None ):
  callLines = []
  line = '%s, %s' % \
    ( call.creationTime, call.name.name )
  for action in call.actions :
    if action.file.name == lfn:
      if status :
        if action.status == status :
          line += '%s%s%s%s'\
            % ( ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
                ', TargetSE %s' % action.targetSE.name if action.targetSE else '',
                ', %s' % action.status,
                ', SequenceID %s' % call.sequenceID )
          if full :
            line += '%s%s%s'\
                % ( ', Extra %s ' % action.extra if action.extra else '',
                ', ErrorCode %s ' % action.errorCode if action.errorCode else '',
                ', ErrorMessage %s ' % action.errorMessage if action.errorMessage else '' )

      else :
        line += '%s%s%s%s'\
            % ( ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
                ', TargetSE %s' % action.targetSE.name if action.targetSE else '',
                ', %s' % action.status,
                ', SequenceID %s' % call.sequenceID )
        if full :
          line += '%s%s%s'\
            % ( ', Extra %s ' % action.extra if action.extra else '',
            ', ErrorCode %s ' % action.errorCode if action.errorCode else '',
            ', ErrorMessage %s ' % action.errorMessage if action.errorMessage else '' )
      callLines.append( line )
  return '\n'.join( callLines )


def printMethodCall( call, full = False, status = None ):
  callLines = []
  line = '%s %s %s' % \
    ( call.creationTime, call.name.name, 'SequenceID %s' % call.sequenceID )
  callLines.append( line )
  for action in call.actions :
    if status :
        if action.status == status :
          line = '\t%s%s%s%s'\
            % ( '%s' % action.status,
            ', File %s' % action.file.name if action.file else '',
            ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
            ', TargetSE %s' % action.targetSE.name if action.targetSE else '' )
          if full :
            line += '%s%s%s'\
              % ( ', Extra %s ' % action.extra if action.extra else '',
              ', ErrorCode %s ' % action.errorCode if action.errorCode else '',
              ', ErrorMessage %s ' % action.errorMessage if action.errorMessage else '' )
    else :
      line = '\t%s%s%s%s'\
        % ( '%s' % action.status,
        ', File %s' % action.file.name if action.file else '',
        ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
        ', TargetSE %s' % action.targetSE.name if action.targetSE else '' )
      if full :
        line += '%s%s%s'\
          % ( ', Extra %s ' % action.extra if action.extra else '',
          ', ErrorCode %s ' % action.errorCode if action.errorCode else '',
          ', ErrorMessage %s ' % action.errorMessage if action.errorMessage else '' )
    callLines.append( line )
  return '\n'.join( callLines )



args = Script.getPositionalArgs()

dlc = DataLoggingClient()

res = dlc.getMethodCall( lfn, name, before, after, status, errorCode )
if res['OK']:
  if not res['Value'] :
    print 'no methodCall to print'
  else :
    print "Found %s method calls" % len( res['Value'] )
    if lfn :
      for call in res['Value'] :
        print printMethodCallLFN( call, lfn, fullFlag, status )
    else :
      for call in res['Value'] :
        print printMethodCall( call, fullFlag, status )
else :
  print 'error %s' % res['Message']

